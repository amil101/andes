/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.store.rdbms;

import org.apache.log4j.Logger;
import org.wso2.andes.configuration.util.ConfigurationProperties;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.DeliverableAndesMetadata;
import org.wso2.andes.kernel.DurableStoreConnection;
import org.wso2.andes.kernel.MessageStore;
import org.wso2.andes.kernel.slot.Slot;
import org.wso2.andes.metrics.MetricsConstants;
import org.wso2.andes.store.AndesDataIntegrityViolationException;
import org.wso2.andes.server.queue.DLCQueueUtils;
import org.wso2.andes.store.cache.AndesMessageCache;
import org.wso2.andes.store.cache.MessageCacheFactory;
import org.wso2.andes.tools.utils.MessageTracer;
import org.wso2.carbon.metrics.manager.Level;
import org.wso2.carbon.metrics.manager.MetricManager;
import org.wso2.carbon.metrics.manager.Timer.Context;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.wso2.andes.store.rdbms.RDBMSConstants.CONTENT_TABLE;
import static org.wso2.andes.store.rdbms.RDBMSConstants.MESSAGE_CONTENT;
import static org.wso2.andes.store.rdbms.RDBMSConstants.MESSAGE_ID;
import static org.wso2.andes.store.rdbms.RDBMSConstants.MSG_OFFSET;
import static org.wso2.andes.store.rdbms.RDBMSConstants.PS_INSERT_MESSAGE_PART;
import static org.wso2.andes.store.rdbms.RDBMSConstants.PS_INSERT_METADATA;
import static org.wso2.andes.store.rdbms.RDBMSConstants.TASK_RETRIEVING_CONTENT_FOR_MESSAGES;

/**
 * ANSI SQL based message store implementation. Message persistence related methods are implemented
 * in this class.
 */
public class RDBMSMessageStoreImpl implements MessageStore {

   
    private static final Logger log = Logger.getLogger(RDBMSMessageStoreImpl.class);

    /**
     * Cache queue name to queue_id mapping to avoid extra sql queries
     */
    private final Map<String, Integer> queueMap;

    private RDBMSConnection rdbmsConnection;

    /**
     * Contains utils methods related to connection health tests
     */
    private RDBMSStoreUtils rdbmsStoreUtils;

    /**
     * the message cache in use ( intension is to optimize reads)
     */
    private AndesMessageCache messageCache;
    
    /**
     * Partially created prepared statement to retrieve content of multiple messages using IN operator
     * this will be completed on the fly when the request comes
     */
    private static final String PS_SELECT_CONTENT_PART =
            "SELECT " + MESSAGE_CONTENT + ", " + MESSAGE_ID + ", " + MSG_OFFSET +
                    " FROM " + CONTENT_TABLE +
                    " WHERE " + MESSAGE_ID + " IN (";

    public RDBMSMessageStoreImpl() {
        queueMap = new ConcurrentHashMap<>();
        
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DurableStoreConnection initializeMessageStore(AndesContextStore contextStore,
                                                         ConfigurationProperties connectionProperties)
            throws AndesException {

        this.rdbmsConnection = new RDBMSConnection();
        // read data source name from config and use
        this.rdbmsConnection.initialize(connectionProperties);
        this.rdbmsStoreUtils = new RDBMSStoreUtils(connectionProperties);
        
        this.messageCache = (new MessageCacheFactory()).create();
        log.info("Message Store initialised");
        return rdbmsConnection;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeMessagePart(List<AndesMessagePart> partList) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        Context messageContentAdditionContext = MetricManager.timer(Level.INFO, MetricsConstants.ADD_MESSAGE_PART).start();
        Context contextWrite = MetricManager.timer(Level.INFO, MetricsConstants.DB_WRITE).start();
        
        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(PS_INSERT_MESSAGE_PART);

            for (AndesMessagePart messagePart : partList) {
                addContentToBatch(preparedStatement, messagePart);
            }
            preparedStatement.executeBatch();
            connection.commit();
        } catch (BatchUpdateException bue) {

            rdbmsStoreUtils.raiseBatchUpdateException(partList, connection, bue,
                    RDBMSConstants.TASK_STORING_MESSAGE_PARTS);

        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_STORING_MESSAGE_PARTS);
            throw rdbmsStoreUtils.convertSQLException("Error occurred while adding message content to DB ", e);
        } finally {
            messageContentAdditionContext.stop();
            contextWrite.stop();
            close(connection, preparedStatement, RDBMSConstants.TASK_STORING_MESSAGE_PARTS);
        }
    }

    /**
     * Adds message content to provided prepared statements batch.
     *
     * @param preparedStatement Prepared statement for storing message content
     * @param messagePart       message content to be stored
     * @throws SQLException
     */
    private void addContentToBatch(PreparedStatement preparedStatement, AndesMessagePart messagePart) throws SQLException {
        preparedStatement.setLong(1, messagePart.getMessageID());
        preparedStatement.setInt(2, messagePart.getOffset());
        preparedStatement.setBytes(3, messagePart.getData());
        preparedStatement.addBatch();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AndesMessagePart getContent(long messageId, int offsetValue) throws AndesException {

        AndesMessagePart messagePart = null;
        Context messageContentRetrievalContext = MetricManager.timer(Level.INFO, MetricsConstants.GET_CONTENT).start();
        try {
            messagePart = getContentFromCache(messageId, offsetValue);
            if (null == messagePart) {
                messagePart = getContentFromStorage(messageId, offsetValue);
            }
        } finally {
            messageContentRetrievalContext.stop();
        }
        return messagePart;
    }

    /**
     * Util method to retrieve a message content from database
     * 
     * @param messageId
     *            message id
     * @param offsetValue
     *            offset value
     * @return a {@link AndesMessagePart} if found in database
     * @throws AndesException
     *             an error
     */
    private AndesMessagePart getContentFromStorage(long messageId, int offsetValue) throws AndesException {
        AndesMessagePart messagePart = null;
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet results = null;
        
        Context contextRead = MetricManager.timer(Level.INFO, MetricsConstants.DB_READ).start();
        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_RETRIEVE_MESSAGE_PART);
            preparedStatement.setLong(1, messageId);
            preparedStatement.setInt(2, offsetValue);
            results = preparedStatement.executeQuery();

            if (results.next()) {
                messagePart = createMessagePart(results, messageId, offsetValue);
            }
        } catch (SQLException e) {
            throw rdbmsStoreUtils.convertSQLException("Error occurred while retrieving message content from DB" +
                                                      " [msg_id= " + messageId + " ]", e);
        } finally {
            contextRead.stop();
            close(connection, preparedStatement, results, RDBMSConstants.TASK_RETRIEVING_MESSAGE_PARTS);
        }
        return messagePart;
    }
    
       
    /**
     * {@inheritDoc}
     */
    @Override
    public Map<Long, List<AndesMessagePart>> getContent(List<Long> messageIDList) throws AndesException {

        Map<Long, List<AndesMessagePart>> contentList = new HashMap<>(messageIDList.size());
        Context messageContentRetrievalContext = MetricManager.timer(Level.INFO, MetricsConstants.GET_CONTENT_BATCH).start();
        try {
            if (messageIDList.isEmpty()) {
                return contentList;
            }

            fillContentFromCache(messageIDList, contentList);

            if (! messageIDList.isEmpty()) {
                fillContentFromStorage(messageIDList, contentList);
            }
            
        } finally {
            messageContentRetrievalContext.stop();
        }

        return contentList;
        
    }
    
    /**
     * Utility method to retrieve content given the list of messages Ids.
     * @param messageIDList message ids 
     * @param contentList this list will be filled with content retrieved from database
     * @throws AndesException an error
     */
    private void fillContentFromStorage(List<Long> messageIDList,
                                        Map<Long, List<AndesMessagePart>> contentList)
                                                                                      throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        Context contextRead = MetricManager.timer(Level.INFO, MetricsConstants.DB_READ).start();
        
         try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(getSelectContentPreparedStmt(messageIDList.size()));
            for (int mesageIDCounter = 0; mesageIDCounter < messageIDList.size(); mesageIDCounter++) {
                preparedStatement.setLong(mesageIDCounter + 1, messageIDList.get(mesageIDCounter));
            }

            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                long messageID = resultSet.getLong(MESSAGE_ID);
                int offset = resultSet.getInt(MSG_OFFSET);
                List<AndesMessagePart> partList = contentList.get(messageID);
                if (null == partList) {
                    partList = new ArrayList<>();
                    contentList.put(messageID, partList);
                }
                AndesMessagePart msgPart = createMessagePart(resultSet, messageID, offset);
                partList.add(msgPart);
            }
            
        } catch (SQLException e) {
            throw rdbmsStoreUtils.convertSQLException("Error occurred while retrieving message content from DB for " +
                    messageIDList.size() + " messages ", e);
        } finally {
            contextRead.stop();
            close(connection, preparedStatement, resultSet, TASK_RETRIEVING_CONTENT_FOR_MESSAGES);
        }
    }

   


    private AndesMessagePart createMessagePart(ResultSet results, long messageId, int offsetValue) throws SQLException {
        byte[] b = results.getBytes(MESSAGE_CONTENT);
        AndesMessagePart messagePart = new AndesMessagePart();
        messagePart.setMessageID(messageId);
        messagePart.setData(b);
        messagePart.setDataLength(b.length);
        messagePart.setOffSet(offsetValue);

        return messagePart;
    }

    /**
     * Create a prepared statement with given number of ? values set to IN operator
     *
     * @param messageCount number of messages that content need to be retrieved from.
     *                     CONDITION: messageCount > 0
     * @return Prepared Statement
     */
    private String getSelectContentPreparedStmt(int messageCount) {

        StringBuilder stmtBuilder = new StringBuilder(PS_SELECT_CONTENT_PART);
        for (int i = 0; i < messageCount - 1; i++) {
            stmtBuilder.append("?,");
        }

        stmtBuilder.append("?)");
        return stmtBuilder.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMetadata(List<AndesMessageMetadata> metadataList) throws AndesException {

        Connection connection = null;
        PreparedStatement preparedStatement = null;

        Context metaDataAdditionContext = MetricManager.timer(Level.INFO, MetricsConstants.ADD_META_DATA_LIST).start();
        Context contextWrite = MetricManager.timer(Level.INFO, MetricsConstants.DB_WRITE).start();

        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(PS_INSERT_METADATA);

            for (AndesMessageMetadata metadata : metadataList) {
                addMetadataToBatch(preparedStatement, metadata, metadata.getStorageQueueName());
            }
            preparedStatement.executeBatch();
            preparedStatement.close();
            addListToExpiryTable(connection, metadataList);
            connection.commit();

            if (log.isDebugEnabled()) {
                log.debug("Metadata list added. Metadata count: " + metadataList.size());
            }
        } catch (BatchUpdateException bue) {

            rdbmsStoreUtils.raiseBatchUpdateException(metadataList, connection, bue, RDBMSConstants.TASK_ADDING_METADATA_LIST);

        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_ADDING_METADATA_LIST);
            throw  rdbmsStoreUtils.convertSQLException("Error occurred while inserting metadata list to queues ", e);
        } finally {
            metaDataAdditionContext.stop();
            contextWrite.stop();
            close(connection, preparedStatement, RDBMSConstants.TASK_ADDING_METADATA_LIST);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMetadata(AndesMessageMetadata metadata) throws AndesException {

        Connection connection = null;
        PreparedStatement preparedStatement = null;

        Context metaDataAdditionContext = MetricManager.timer(Level.INFO, MetricsConstants.ADD_META_DATA).start();
        Context contextWrite = MetricManager.timer(Level.INFO, MetricsConstants.DB_WRITE).start();

        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(PS_INSERT_METADATA);

            addMetadataToBatch(preparedStatement, metadata, metadata.getStorageQueueName());
            preparedStatement.executeBatch();
            preparedStatement.close();
            addToExpiryTable(connection, metadata);
            connection.commit();

            if (log.isDebugEnabled()) {
                log.debug("Metadata added: msgID: " + metadata.getMessageID() +
                        " Destination: " + metadata.getStorageQueueName());
            }
        }/* catch (BatchUpdateException bue) {
            
           int[] updateCountsOfFailedBatch = bue.getUpdateCounts();
           
           for ( int i = 0;  i < updateCountsOfFailedBatch.length ; i ++){
               if ( Statement.EXECUTE_FAILED == updateCountsOfFailedBatch[i]){
                    log.error(String.format("couldn't save the message meta data : message id: %d, offset : %d, storage queue name: %s",
                                            metadata.getMessageID(), metadata.getStorageQueueName()));
               }
           }

           rollback(connection, RDBMSConstants.TASK_ADDING_METADATA);
           throw new AndesException("Error occurred while inserting metadata list to queues ", bue);
            
        }*/ catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_ADDING_METADATA);
        } finally {
            metaDataAdditionContext.stop();
            contextWrite.stop();
            close(connection, preparedStatement, RDBMSConstants.TASK_ADDING_METADATA);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeMessages(List<AndesMessage> messageList) throws AndesException {
        Connection connection = null;
        PreparedStatement storeMetadataPS = null;
        PreparedStatement storeContentPS = null;

        try {

            connection = getConnection();
            storeMetadataPS = connection.prepareStatement(PS_INSERT_METADATA);
            storeContentPS = connection.prepareStatement(PS_INSERT_MESSAGE_PART);

            for (AndesMessage message : messageList) {
                
                addToCache(message);
                addMetadataToBatch(storeMetadataPS,
                        message.getMetadata(),
                        message.getMetadata().getStorageQueueName());

                for (AndesMessagePart messagePart : message.getContentChunkList()) {
                    addContentToBatch(storeContentPS, messagePart);
                }
            }

            storeMetadataPS.executeBatch();
            storeContentPS.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_ADDING_METADATA);
            throw rdbmsStoreUtils.convertSQLException("Error occurred while inserting messages to queue ", e);
        } finally {
            close(storeMetadataPS, RDBMSConstants.TASK_ADDING_MESSAGES);
            close(storeContentPS, RDBMSConstants.TASK_ADDING_MESSAGES);
            close(connection, RDBMSConstants.TASK_ADDING_MESSAGES);
        }
    }

    
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void addMetadataToQueue(String queueName, AndesMessageMetadata metadata)
            throws AndesException {

        Connection connection = null;
        PreparedStatement preparedStatement = null;

        Context metaAdditionToQueueContext = MetricManager.timer(Level.INFO, MetricsConstants.ADD_META_DATA_TO_QUEUE).start();
        Context contextWrite = MetricManager.timer(Level.INFO, MetricsConstants.DB_WRITE).start();

        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(PS_INSERT_METADATA);

            // add to metadata table
            addMetadataToBatch(preparedStatement, metadata, queueName);
            preparedStatement.executeBatch();
            preparedStatement.close();
            addToExpiryTable(connection, metadata);

            connection.commit();
        }/* catch (BatchUpdateException bue) {
            
           int[] updateCountsOfFailedBatch = bue.getUpdateCounts();
           
           for ( int i = 0;  i < updateCountsOfFailedBatch.length ; i ++){
               if ( Statement.EXECUTE_FAILED == updateCountsOfFailedBatch[i]){
                    log.error(String.format("couldn't save the message meta data : message id: %d, offset : %d, storage queue name: %s",
                                            metadata.getMessageID(), metadata.getStorageQueueName()));
               }
           }

           rollback(connection, RDBMSConstants.TASK_ADDING_METADATA_TO_QUEUE + queueName);
           throw new AndesException("Error occurred while inserting message metadata to queue ", bue);
            
        }*/ catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_ADDING_METADATA_TO_QUEUE + queueName);
            throw rdbmsStoreUtils.convertSQLException(
                    "Error occurred while inserting message metadata to queue " + queueName, e);
        } finally {
            metaAdditionToQueueContext.stop();
            contextWrite.stop();
            close(connection, preparedStatement, RDBMSConstants.TASK_ADDING_METADATA_TO_QUEUE + queueName);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMetadataToQueue(String queueName, List<AndesMessageMetadata> metadataList)
            throws AndesException {

        Connection connection = null;
        PreparedStatement preparedStatement = null;

        Context metaAddditionToQueuecontext = MetricManager.timer(Level.INFO, MetricsConstants.ADD_META_DATA_TO_QUEUE_LIST).start();
        Context contextWrite = MetricManager.timer(Level.INFO, MetricsConstants.DB_WRITE).start();

        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(PS_INSERT_METADATA);

            for (AndesMessageMetadata md : metadataList) {
                addMetadataToBatch(preparedStatement, md, queueName);
            }

            preparedStatement.executeBatch();
            preparedStatement.close();
            addListToExpiryTable(connection, metadataList);
            connection.commit();

        } catch (BatchUpdateException bue) {
            rdbmsStoreUtils.raiseBatchUpdateException(metadataList, connection, bue,
                    RDBMSConstants.TASK_ADDING_METADATA_LIST_TO_QUEUE + queueName);
        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_ADDING_METADATA_LIST_TO_QUEUE + queueName);
            throw rdbmsStoreUtils.convertSQLException(
                    "Error occurred while inserting message metadata list to queue " + queueName, e);
        } finally {
            metaAddditionToQueuecontext.stop();
            contextWrite.stop();
            close(connection, preparedStatement, RDBMSConstants.TASK_ADDING_METADATA_LIST_TO_QUEUE + queueName);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void moveMetadataToQueue(long messageId, String currentQueueName, String targetQueueName)
            throws AndesException {

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        Context contextWrite = MetricManager.timer(Level.INFO, MetricsConstants.DB_WRITE).start();
        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_UPDATE_METADATA_QUEUE);

            preparedStatement.setInt(1, getCachedQueueID(targetQueueName));
            preparedStatement.setLong(2, messageId);
            preparedStatement.setInt(3, getCachedQueueID(currentQueueName));
            preparedStatement.execute();
            preparedStatement.close();

            connection.commit();
        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_UPDATING_META_DATA_QUEUE + targetQueueName);
            throw rdbmsStoreUtils.convertSQLException("Error occurred while updating message metadata to destination queue "
                    + targetQueueName, e);
        } finally {
            contextWrite.stop();
            close(connection, preparedStatement, RDBMSConstants.TASK_UPDATING_META_DATA_QUEUE + targetQueueName);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void moveMetadataToDLC(long messageId, String dlcQueueName) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;

        Context moveMetadataToDLCContext = MetricManager.timer(Level.INFO, MetricsConstants.MOVE_METADATA_TO_DLC)
                .start();

        //Remove the message from cache
        removeFromCache(messageId);

        Context contextWrite = MetricManager.timer(Level.INFO, MetricsConstants.DB_WRITE).start();
        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_MOVE_METADATA_TO_DLC);
            preparedStatement.setInt(1, getCachedQueueID(dlcQueueName));
            preparedStatement.setLong(2, messageId);
            preparedStatement.execute();
            connection.commit();
        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_MOVING_METADATA_TO_DLC);
            throw rdbmsStoreUtils.convertSQLException("Error occurred while moving message metadata to dead letter " +
                    "channel.", e);
        } finally {
            contextWrite.stop();
            moveMetadataToDLCContext.stop();
            close(connection, preparedStatement, RDBMSConstants.TASK_MOVING_METADATA_TO_DLC);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void moveMetadataToDLC(List<AndesMessageMetadata> messages, String dlcQueueName) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;

        Context moveMetadataToDLCContext = MetricManager.timer(Level.INFO, MetricsConstants.MOVE_METADATA_TO_DLC)
                .start();
        Context contextWrite = MetricManager.timer(Level.INFO, MetricsConstants.DB_WRITE).start();
        List<Long> messageIDsToRemoveFromCache = new ArrayList<>(messages.size());

        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_MOVE_METADATA_TO_DLC);
            for (AndesMessageMetadata message : messages) {
                messageIDsToRemoveFromCache.add(message.getMessageID());
                preparedStatement.setInt(1, getCachedQueueID(dlcQueueName));
                preparedStatement.setLong(2, message.getMessageID());
                preparedStatement.addBatch();
            }

            //remove messages from cache
            removeFromCache(messageIDsToRemoveFromCache);

            preparedStatement.executeBatch();
            connection.commit();

        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_MOVING_METADATA_TO_DLC);
            throw rdbmsStoreUtils.convertSQLException("Error occurred while moving message metadata to dead letter " +
                    "channel.", e);
        } finally {
            contextWrite.stop();
            moveMetadataToDLCContext.stop();
            close(connection, preparedStatement, RDBMSConstants.TASK_MOVING_METADATA_TO_DLC);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateMetadataInformation(String currentQueueName, List<AndesMessageMetadata> metadataList) throws
            AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;

        Context metaUpdateContext = MetricManager.timer(Level.INFO, MetricsConstants.UPDATE_META_DATA_INFORMATION).start();
        Context contextWrite = MetricManager.timer(Level.INFO, MetricsConstants.DB_WRITE).start();

        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_UPDATE_METADATA);

            for (AndesMessageMetadata metadata : metadataList) {
                preparedStatement.setInt(1, getCachedQueueID(metadata.getStorageQueueName()));
                preparedStatement.setBytes(2, metadata.getMetadata());
                preparedStatement.setLong(3, metadata.getMessageID());
                preparedStatement.setInt(4, getCachedQueueID(currentQueueName));
                preparedStatement.addBatch();
            }

            preparedStatement.executeBatch();
            preparedStatement.close();
            addListToExpiryTable(connection, metadataList);

            connection.commit();
        } catch (BatchUpdateException bue) {
            rdbmsStoreUtils.raiseBatchUpdateException(metadataList, connection, bue,
                    RDBMSConstants.TASK_UPDATING_META_DATA);

        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_UPDATING_META_DATA);
            throw rdbmsStoreUtils.convertSQLException("Error occurred while updating message metadata list.", e);
        } finally {
            metaUpdateContext.stop();
            contextWrite.stop();
            close(connection, preparedStatement, RDBMSConstants.TASK_UPDATING_META_DATA);
        }
    }

    /**
     * Adds a single metadata to a batch insert of metadata.
     *
     * @param preparedStatement prepared statement to add messages to metadata table
     * @param metadata          AndesMessageMetadata
     * @param queueName         queue to be assigned
     * @throws SQLException
     */
    private void addMetadataToBatch(PreparedStatement preparedStatement,
                                    AndesMessageMetadata metadata,
                                    final String queueName) throws SQLException {

        Context metaAdditionToBatchContext = MetricManager.timer(Level.INFO, MetricsConstants.ADD_META_DATA_TO_BATCH).start();
        Context contextWrite = MetricManager.timer(Level.INFO, MetricsConstants.DB_WRITE).start();
        try {
            preparedStatement.setLong(1, metadata.getMessageID());
            preparedStatement.setInt(2, getCachedQueueID(queueName));
            preparedStatement.setBytes(3, metadata.getMetadata());
            preparedStatement.addBatch();
        } finally {
            metaAdditionToBatchContext.stop();
            contextWrite.stop();
        }


    }

    /**
     * Add metadata entry to expiry table.
     *
     * @param connection SQLConnection. Connection resource is not closed within the method
     * @param metadata   AndesMessageMetadata
     * @throws SQLException
     */
    private void addToExpiryTable(Connection connection, AndesMessageMetadata metadata)
            throws SQLException {
        PreparedStatement preparedStatement = null;
        Context contextWrite = MetricManager.timer(Level.INFO, MetricsConstants.DB_WRITE).start();
        try {
            if (metadata.getExpirationTime() > 0) {
                preparedStatement = connection.prepareStatement(RDBMSConstants.PS_INSERT_EXPIRY_DATA);
                addExpiryTableEntryToBatch(preparedStatement, metadata);
                preparedStatement.executeBatch();
                connection.commit();
            }
        } finally {
            contextWrite.stop();
            close(preparedStatement, "adding entry to expiry table");
        }
    }

    /**
     * Add a list of metadata entries to expiry table
     *
     * @param connection SQLConnection. Connection resource is not closed within the method
     * @param list       AndesMessageMetadata list
     * @throws SQLException
     */
    private void addListToExpiryTable(Connection connection, List<AndesMessageMetadata> list)
            throws SQLException {

        PreparedStatement preparedStatement = null;
        Context contextWrite = MetricManager.timer(Level.INFO, MetricsConstants.DB_WRITE).start();
        try {
            preparedStatement = connection
                    .prepareStatement(RDBMSConstants.PS_INSERT_EXPIRY_DATA);

            for (AndesMessageMetadata andesMessageMetadata : list) {
                if (andesMessageMetadata.getExpirationTime() > 0) {
                    addExpiryTableEntryToBatch(preparedStatement, andesMessageMetadata);
                }
            }
            preparedStatement.executeBatch();
            connection.commit();
        } finally {
            contextWrite.stop();
            close(preparedStatement, "adding list to expiry table");
        }
    }

    /**
     * Does a batch update on the given prepared statement to add entries to expiry table.
     *
     * @param preparedStatement PreparedStatement. Object is not closed within the method
     * @param metadata          AndesMessageMetadata
     * @throws SQLException
     */
    private void addExpiryTableEntryToBatch(PreparedStatement preparedStatement,
                                            AndesMessageMetadata metadata) throws SQLException {
        preparedStatement.setLong(1, metadata.getMessageID());
        preparedStatement.setLong(2, metadata.getExpirationTime());
        preparedStatement.setString(3, metadata.getStorageQueueName());
        preparedStatement.addBatch();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AndesMessageMetadata getMetadata(long messageId) throws AndesException {
        
        //Check if cache contains this message.
        AndesMessage cached = getMessageFromCache(messageId);
        if (null != cached){
            return cached.getMetadata();
        }
        
        AndesMessageMetadata md = null;
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet results = null;
        
        Context metaRetrievalContext = MetricManager.timer(Level.INFO, MetricsConstants.GET_META_DATA).start();
        Context contextRead = MetricManager.timer(Level.INFO, MetricsConstants.DB_READ).start();

        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_SELECT_METADATA);
            preparedStatement.setLong(1, messageId);
            results = preparedStatement.executeQuery();
            if (results.next()) {
                byte[] b = results.getBytes(RDBMSConstants.METADATA);
                md = new AndesMessageMetadata(messageId, b, true);
            }
        } catch (SQLException e) {
            throw rdbmsStoreUtils.convertSQLException("error occurred while retrieving message " +
                    "metadata for msg id:" + messageId, e);
        } finally {
            metaRetrievalContext.stop();
            contextRead.stop();
            close(connection, preparedStatement, results, RDBMSConstants.TASK_RETRIEVING_METADATA + messageId);
        }
        return md;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<DeliverableAndesMetadata> getMetadataList(Slot slot, final String storageQueueName, long firstMsgId,
                                                      long lastMsgID) throws AndesException {

        List<DeliverableAndesMetadata> metadataList = new ArrayList<>();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        Context metaListRetrievalContext = MetricManager.timer(Level.INFO, MetricsConstants.GET_META_DATA_LIST).start();
        Context contextRead = MetricManager.timer(Level.INFO, MetricsConstants.DB_READ).start();

        try {
            connection = getConnection();
            preparedStatement = connection
                    .prepareStatement(RDBMSConstants.PS_SELECT_METADATA_RANGE_FROM_QUEUE);
            preparedStatement.setInt(1, getCachedQueueID(storageQueueName));
            preparedStatement.setLong(2, firstMsgId);
            preparedStatement.setLong(3, lastMsgID);

            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                DeliverableAndesMetadata md = new DeliverableAndesMetadata(slot,
                        resultSet.getLong(RDBMSConstants.MESSAGE_ID),
                        resultSet.getBytes(RDBMSConstants.METADATA),
                        true
                );
                md.setStorageQueueName(storageQueueName);
                metadataList.add(md);
                //Tracing message
                MessageTracer.trace(md, MessageTracer.METADATA_READ_FROM_DB);
            }
            if (log.isDebugEnabled()) {
                log.debug("request: metadata range (" + firstMsgId + " , " + lastMsgID +
                        ") in destination queue " + storageQueueName
                        + "\nresponse: metadata count " + metadataList.size());
            }
        } catch (SQLException e) {
            throw rdbmsStoreUtils.convertSQLException("Error occurred while retrieving messages between msg id " + firstMsgId + " and " +
                    lastMsgID + " from queue " + storageQueueName, e);
        } finally {
            metaListRetrievalContext.stop();
            contextRead.stop();
            close(connection, preparedStatement, resultSet,
                  RDBMSConstants.TASK_RETRIEVING_METADATA_RANGE_FROM_QUEUE + storageQueueName);
        }
        return metadataList;
    }
    
    /**
     * {@inheritDoc}
     */
    public List<Long> getNextNMessageIdsFromQueue(final String storageQueueName,
                                                  long firstMsgId, int count)
                                                                              throws AndesException {
        List<Long> mdList = new ArrayList<>(count);
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet results = null;

        Context nextMessageIdsRetrievalContext = MetricManager.timer(Level.INFO,
                                                                     MetricsConstants.GET_NEXT_MESSAGE_IDS_FROM_QUEUE)
                                                                                                               .start();
        Context contextRead = MetricManager.timer(Level.INFO, MetricsConstants.DB_READ).start();

        try {
            connection = getConnection();
            preparedStatement = connection
                                          .prepareStatement(RDBMSConstants.PS_SELECT_MESSAGE_IDS_FROM_QUEUE);
            preparedStatement.setLong(1, firstMsgId - 1);
            preparedStatement.setInt(2, getCachedQueueID(storageQueueName));

            results = preparedStatement.executeQuery();
            int resultCount = 0;
            while (results.next()) {

                if (resultCount == count) {
                    break;
                }

                Long messageId = results.getLong(RDBMSConstants.MESSAGE_ID);

                mdList.add(messageId);
                resultCount++;
            }
        } catch (SQLException e) {
            throw rdbmsStoreUtils.convertSQLException("error occurred while retrieving message ids from queue ", e);
        } finally {
            nextMessageIdsRetrievalContext.stop();
            contextRead.stop();
            close(connection, preparedStatement, results, RDBMSConstants.TASK_RETRIEVING_NEXT_N_IDS_FROM_QUEUE);
        }
        return mdList;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(final String storageQueueName,
                                                                       long firstMsgId, int count)
            throws AndesException {

        List<AndesMessageMetadata> mdList = new ArrayList<>(count);
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet results = null;


        Context nextMetaRetrievalContext = MetricManager.timer(Level.INFO, MetricsConstants.GET_NEXT_MESSAGE_METADATA_FROM_QUEUE).start();
        Context contextRead = MetricManager.timer(Level.INFO, MetricsConstants.DB_READ).start();

        try {
            connection = getConnection();
            preparedStatement = connection
                    .prepareStatement(RDBMSConstants.PS_SELECT_METADATA_FROM_QUEUE);
            preparedStatement.setLong(1, firstMsgId - 1);
            preparedStatement.setInt(2, getCachedQueueID(storageQueueName));

            results = preparedStatement.executeQuery();
            int resultCount = 0;
            while (results.next()) {

                if (resultCount == count) {
                    break;
                }

                AndesMessageMetadata md = new AndesMessageMetadata(
                        results.getLong(RDBMSConstants.MESSAGE_ID),
                        results.getBytes(RDBMSConstants.METADATA),
                        true
                );
                md.setStorageQueueName(storageQueueName);
                mdList.add(md);
                resultCount++;
            }
        } catch (SQLException e) {
            throw rdbmsStoreUtils.convertSQLException("error occurred while retrieving message metadata from queue ", e);
        } finally {
            nextMetaRetrievalContext.stop();
            contextRead.stop();
            close(connection, preparedStatement, results, RDBMSConstants.TASK_RETRIEVING_NEXT_N_METADATA_FROM_QUEUE);
        }
        return mdList;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataForQueueFromDLC(String storageQueueName,
                                                                             String dlcQueueName, long firstMsgId,
                                                                             int count) throws AndesException {

        List<AndesMessageMetadata> mdList = new ArrayList<>(count);
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet results = null;

        Context nextMetaRetrievalContext = MetricManager.timer(Level.INFO, MetricsConstants
                .GET_NEXT_MESSAGE_METADATA_IN_DLC_FOR_QUEUE).start();
        Context contextRead = MetricManager.timer(Level.INFO, MetricsConstants.DB_READ).start();

        try {
            connection = getConnection();
            preparedStatement = connection
                    .prepareStatement(RDBMSConstants.PS_SELECT_METADATA_IN_DLC_FOR_QUEUE);
            preparedStatement.setLong(1, firstMsgId - 1);
            preparedStatement.setInt(2, getCachedQueueID(storageQueueName));
            preparedStatement.setInt(3, getCachedQueueID(dlcQueueName));
            results = preparedStatement.executeQuery();
            int resultCount = 0;
            while (results.next()) {

                if (resultCount == count) {
                    break;
                }

                AndesMessageMetadata md = new AndesMessageMetadata(
                        results.getLong(RDBMSConstants.MESSAGE_ID),
                        results.getBytes(RDBMSConstants.METADATA),
                        true
                );
                md.setStorageQueueName(storageQueueName);
                mdList.add(md);
                resultCount++;
            }
        } catch (SQLException e) {
            throw rdbmsStoreUtils.convertSQLException("error occurred while retrieving message metadata from queue ",
                    e);
        } finally {
            nextMetaRetrievalContext.stop();
            contextRead.stop();
            close(connection, preparedStatement, results,
                  RDBMSConstants.TASK_RETRIEVING_NEXT_N_METADATA_IN_DLC_FOR_QUEUE);
        }
        return mdList;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataFromDLC(String dlcQueueName, long firstMsgId, int count)
            throws AndesException {
        List<AndesMessageMetadata> mdList = new ArrayList<>(count);
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet results = null;

        Context nextMetaRetrievalContext = MetricManager.timer(Level.INFO, MetricsConstants
                .GET_NEXT_MESSAGE_METADATA_IN_DLC).start();
        Context contextRead = MetricManager.timer(Level.INFO, MetricsConstants.DB_READ).start();

        try {
            connection = getConnection();
            preparedStatement = connection
                    .prepareStatement(RDBMSConstants.PS_SELECT_METADATA_IN_DLC);
            preparedStatement.setLong(1, firstMsgId - 1);
            preparedStatement.setInt(2, getCachedQueueID(dlcQueueName));
            results = preparedStatement.executeQuery();
            int resultCount = 0;
            while (results.next()) {

                if (resultCount == count) {
                    break;
                }

                AndesMessageMetadata md = new AndesMessageMetadata(
                        results.getLong(RDBMSConstants.MESSAGE_ID),
                        results.getBytes(RDBMSConstants.METADATA),
                        true
                );
                mdList.add(md);
                resultCount++;
            }
        } catch (SQLException e) {
            throw rdbmsStoreUtils.convertSQLException("error occurred while retrieving message metadata from queue ",
                    e);
        } finally {
            nextMetaRetrievalContext.stop();
            contextRead.stop();
            close(connection, preparedStatement, results, RDBMSConstants.TASK_RETRIEVING_NEXT_N_METADATA_FROM_DLC);
        }
        return mdList;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessageMetadataFromQueue(final String storageQueueName,
                                               List<AndesMessageMetadata> messagesToRemove)
            throws AndesException {

        Connection connection = null;
        PreparedStatement preparedStatement = null;

        Context metaDeletionContext = MetricManager.timer(Level.INFO, MetricsConstants.DELETE_MESSAGE_META_DATA_FROM_QUEUE).start();
        Context contextWrite = MetricManager.timer(Level.INFO, MetricsConstants.DB_WRITE).start();

        try {
            int queueID = getCachedQueueID(storageQueueName);

            connection = getConnection();
            preparedStatement = connection
                    .prepareStatement(RDBMSConstants.PS_DELETE_METADATA_FROM_QUEUE);
            for (AndesMessageMetadata messageID : messagesToRemove) {
                preparedStatement.setInt(1, queueID);
                preparedStatement.setLong(2, messageID.getMessageID());
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            connection.commit();

            if (log.isDebugEnabled()) {
                log.debug("Metadata removed. " + messagesToRemove.size() +
                        " metadata from destination " + storageQueueName);
            }
        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_DELETING_METADATA_FROM_QUEUE + storageQueueName);
            throw rdbmsStoreUtils.convertSQLException("error occurred while deleting message metadata from queue ",
                    e);
        } finally {
            metaDeletionContext.stop();
            contextWrite.stop();
            close(connection, preparedStatement, RDBMSConstants.TASK_DELETING_METADATA_FROM_QUEUE + storageQueueName);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessages(final String storageQueueName,
                               List<AndesMessageMetadata> messagesToRemove)
            throws AndesException {
        Connection connection = null;
        PreparedStatement metadataRemovalPreparedStatement = null;

        Context messageDeletionContext = MetricManager.timer(Level.INFO, MetricsConstants
                .DELETE_MESSAGE_META_DATA_AND_CONTENT).start();
        Context contextWrite = MetricManager.timer(Level.INFO, MetricsConstants.DB_WRITE).start();

        try {

            List<Long> messageIDsToRemoveFromCache = new ArrayList<>(messagesToRemove.size());
            connection = getConnection();

            //Since referential integrity is imposed on the two tables: message content and metadata,
            //deleting message metadata will cause message content to be automatically deleted
            metadataRemovalPreparedStatement = connection.prepareStatement(RDBMSConstants.PS_DELETE_METADATA);

                for (AndesMessageMetadata message : messagesToRemove) {
                    //add parameters to delete metadata
                    messageIDsToRemoveFromCache.add(message.getMessageID());
                    metadataRemovalPreparedStatement.setLong(1, message.getMessageID());
                    metadataRemovalPreparedStatement.addBatch();
            }

            removeFromCache(messageIDsToRemoveFromCache);
            metadataRemovalPreparedStatement.executeBatch();
            connection.commit();

            if (log.isDebugEnabled()) {
                log.debug("Metadata and content removed: " + messagesToRemove.size()
                          + " for destination queue:" + storageQueueName);
            }
        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_DELETING_METADATA_FROM_QUEUE + storageQueueName
                     + " and " + RDBMSConstants.TASK_DELETING_MESSAGE_PARTS);
            throw rdbmsStoreUtils.convertSQLException("error occurred while deleting message metadata and content for "
                                                      + "queue ", e);
        } finally {
            messageDeletionContext.stop();
            contextWrite.stop();
            close(connection, metadataRemovalPreparedStatement, RDBMSConstants.TASK_DELETING_METADATA_FROM_QUEUE
                  + storageQueueName + " and " + RDBMSConstants.TASK_DELETING_MESSAGE_PARTS);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageMetadata> getExpiredMessages(int limit) throws AndesException {

        // todo: can't we just delete expired messages?
        Connection connection = null;
        List<AndesMessageMetadata> list = new ArrayList<>(limit);
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        Context contextRead = MetricManager.timer(Level.INFO, MetricsConstants.DB_READ).start();

        try {
            connection = getConnection();

            // get expired message list
            preparedStatement = connection
                    .prepareStatement(RDBMSConstants.PS_SELECT_EXPIRED_MESSAGES);
            resultSet = preparedStatement.executeQuery();
            int resultCount = 0;
            while (resultSet.next()) {

                if (resultCount == limit) {
                    break;
                }
                AndesMessageMetadata metadata = new AndesMessageMetadata(
                        resultSet.getLong(RDBMSConstants.MESSAGE_ID),
                        resultSet.getBytes(RDBMSConstants.METADATA),
                        true);
                metadata.setStorageQueueName(null);
                list.add(metadata);
                resultCount++;
            }
            return list;
        } catch (SQLException e) {
            throw rdbmsStoreUtils.convertSQLException("error occurred while retrieving expired messages.", e);
        } finally {
            contextRead.stop();
            close(connection, preparedStatement, resultSet, RDBMSConstants.TASK_RETRIEVING_EXPIRED_MESSAGES);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {

    }

    /**
     * This method is expected to be used in a transaction based update.
     *
     * @param connection       connection to be used
     * @param messagesToRemove AndesRemovableMetadata
     * @throws SQLException
     */
    private void deleteFromExpiryQueue(Connection connection,
                                       List<AndesMessageMetadata> messagesToRemove)
            throws SQLException {

        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_DELETE_EXPIRY_DATA);
            for (AndesMessageMetadata md : messagesToRemove) {
                preparedStatement.setLong(1, md.getMessageID());
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();

        } finally {
            close(preparedStatement, RDBMSConstants.TASK_DELETING_FROM_EXPIRY_TABLE);
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessagesFromExpiryQueue(List<Long> messagesToRemove) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        Context contextWrite = MetricManager.timer(Level.INFO, MetricsConstants.DB_WRITE).start();

        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_DELETE_EXPIRY_DATA);
            for (Long mid : messagesToRemove) {
                preparedStatement.setLong(1, mid);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_DELETING_FROM_EXPIRY_TABLE);
            throw rdbmsStoreUtils.convertSQLException("error occurred while deleting message metadata " +
                    "from expiration table ", e);
        } finally {
            contextWrite.stop();
            close(connection, preparedStatement, RDBMSConstants.TASK_DELETING_FROM_EXPIRY_TABLE);
        }
    }

    /**
     * This method caches the queue ids for destination queue names. If queried destination queue is
     * not in cache updates the cache and returns the queue id.
     *
     * @param destinationQueueName queue name
     * @return corresponding queue id for the destination queue. On error -1 is returned
     * @throws SQLException
     */
    private int getCachedQueueID(final String destinationQueueName) throws SQLException {

        // get from map
        Integer id = queueMap.get(destinationQueueName);
        if (id != null) {
            return id;
        }

        // If not in cached map. query from DB (some other node might have created it)
        // If queue is not available create a queue in DB
        int queueID = getQueueID(destinationQueueName);

        if (queueID != -1) {
            queueMap.put(destinationQueueName, queueID);
        }
        return queueID;
    }

    /**
     * Retrieved the queue ID from DB. If the ID is not present create a new queue and get the id.
     *
     * @param destinationQueueName queue name
     * @return queue id
     * @throws SQLException
     */
    private int getQueueID(final String destinationQueueName) throws SQLException {

        int queueID = -1;
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        Context contextRead = MetricManager.timer(Level.INFO, MetricsConstants.DB_READ).start();
        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_SELECT_QUEUE_ID);
            preparedStatement.setString(1, destinationQueueName);
            resultSet = preparedStatement.executeQuery();

            // ResultSet.first() is not supported by MS SQL hence using next()
            if (resultSet.next()) {
                queueID = resultSet.getInt(RDBMSConstants.QUEUE_ID);
            }
            resultSet.close();

            // If queue is not present create a new queue entry
            if (queueID == -1) {
                createNewQueue(connection, destinationQueueName);
            }

            // Get the resultant ID.
            // NOTE: In different DB implementations getting the auto generated queue id differs in subtle ways
            // Hence doing a simple select again after adding the entry to DB
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                queueID = resultSet.getInt(RDBMSConstants.QUEUE_ID);
            }

        } catch (SQLException e) {
            log.error("Error occurred while retrieving destination queue id " +
                    "for destination queue " + destinationQueueName, e);
            throw e;
        } finally {
            contextRead.stop();
            close(connection, preparedStatement, resultSet,
                  RDBMSConstants.TASK_RETRIEVING_QUEUE_ID + destinationQueueName);
        }
        return queueID;
    }

    /**
     * Using the provided connection create a new queue with queue id in database
     *
     * @param connection           Connection
     * @param destinationQueueName queue name
     * @throws SQLException
     */
    private void createNewQueue(final Connection connection, final String destinationQueueName) throws SQLException {

        PreparedStatement preparedStatement = null;
        Context contextWrite = MetricManager.timer(Level.INFO, MetricsConstants.DB_WRITE).start();

        try {

            //Add the queue to the database
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_INSERT_QUEUE);
            preparedStatement.setString(1, destinationQueueName);
            preparedStatement.executeUpdate();
            connection.commit();
            preparedStatement.close();

            //Add the queue to the internal map as well
            int queueId = getQueueID(destinationQueueName);
            if (-1 != queueId) {
                queueMap.put(destinationQueueName, queueId);
            }

        } catch (SQLException e) {
            AndesException andesException =
                    rdbmsStoreUtils.convertSQLException("Error occurred while creating queue", e);

            if(andesException instanceof AndesDataIntegrityViolationException) {
                // This exception occurred because some other node has created the queue in parallel.
                // Therefore no need to create the queue. It's already created.
                // Nothing need to be done if this exception occur.
                log.warn("Queue already created. Skipping insert destination queue ["+ destinationQueueName +
                         "] to database ");
            } else {
                log.error("Error occurred while inserting destination queue [" + destinationQueueName +
                          "] to database ");
                throw e;
            }
        } finally {
            contextWrite.stop();
            String task = RDBMSConstants.TASK_CREATING_QUEUE + destinationQueueName;
            close(preparedStatement, task);
        }
    }

    /**
     * Returns SQL Connection object from connection pooled data source.
     *
     * @return Connection
     * @throws SQLException
     */
    protected Connection getConnection() throws SQLException {
        return rdbmsConnection.getDataSource().getConnection();
    }

    /**
     * Closes the provided connection if it is open. on failure log the error;
     *
     * @param connection Connection
     * @param task       task that was done before closing
     */
    protected void close(Connection connection, String task) {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            log.error("Failed to close connection after " + task, e);
        }

    }

    /**
     * On database update failure tries to rollback
     *
     * @param connection database connection
     * @param task       explanation of the task done when the rollback was triggered
     */
    protected void rollback(Connection connection, String task) {
        if (connection != null) {
            try {
                connection.rollback();
            } catch (SQLException e) {
                log.warn("Rollback failed on " + task, e);
            }
        }
    }

    /**
     * Close the prepared statement resource.
     *
     * @param preparedStatement PreparedStatement
     * @param task              task that was done by the closed prepared statement.
     */
    protected void close(PreparedStatement preparedStatement, String task) {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                log.error("Closing prepared statement failed after " + task, e);
            }
        }
    }

    /**
     * close the connection and the prepared statement.
     *
     * @param connection        connection to be closed
     * @param preparedStatement PreparedStatement
     * @param task              task that was done by the closed prepared statement.
     */
    private void close(Connection connection, PreparedStatement preparedStatement, String task) {
        close(connection, task);
        close(preparedStatement, task);
    }

    /**
     * close the connection, prepared statement and the result set.
     *
     * @param connection        connection to be closed
     * @param preparedStatement PreparedStatement
     * @param resultSet         result set to be closed
     * @param task              task that was done by the closed prepared statement.
     */
    private void close(Connection connection, PreparedStatement preparedStatement, ResultSet resultSet, String task) {
        close(connection, task);
        close(preparedStatement, task);
        close(resultSet, task);
    }

    /**
     * closes the result set resources
     *
     * @param resultSet ResultSet
     * @param task      task that was done by the closed result set.
     */
    protected void close(ResultSet resultSet, String task) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                log.error("Closing result set failed after " + task, e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMessageToExpiryQueue(Long messageId, Long expirationTime,
                                        boolean isMessageForTopic, String destination)
            throws AndesException {
        // NOTE: Feature Message Expiration moved to a future release
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int deleteAllMessageMetadata(String storageQueueName) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        Context contextWrite = MetricManager.timer(Level.INFO, MetricsConstants.DB_WRITE).start();
        int deletedMessagecount = 0;
        try {
            int queueID = getCachedQueueID(storageQueueName);

            connection = getConnection();
            preparedStatement = connection
                    .prepareStatement(RDBMSConstants.PS_CLEAR_QUEUE_FROM_METADATA);
            preparedStatement.setInt(1, queueID);
            deletedMessagecount = preparedStatement.executeUpdate();
            connection.commit();
            if (log.isDebugEnabled()) {
                log.debug("DELETED all message metadata from " + storageQueueName +
                        " with queue ID " + queueID);
            }
        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_DELETING_METADATA_FROM_QUEUE + storageQueueName);
            throw rdbmsStoreUtils.convertSQLException("error occurred while clearing message metadata from queue :" +
                    storageQueueName, e);
        } finally {
            contextWrite.stop();
            close(connection, preparedStatement, RDBMSConstants.TASK_DELETING_METADATA_FROM_QUEUE + storageQueueName);
        }
        return deletedMessagecount;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int clearDLCQueue(String dlcQueueName) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        Context contextWrite = MetricManager.timer(Level.INFO, MetricsConstants.DB_WRITE).start();
        int deletedMessagecount = 0;
        try {
            int queueID = getCachedQueueID(dlcQueueName);

            connection = getConnection();
            preparedStatement = connection
                    .prepareStatement(RDBMSConstants.PS_CLEAR_DLC_QUEUE);
            preparedStatement.setInt(1, queueID);

            deletedMessagecount = preparedStatement.executeUpdate();
            connection.commit();

            if (log.isDebugEnabled()) {
                log.debug("DELETED all message metadata for dlc queue " + dlcQueueName +
                        " with queue ID " + queueID);
            }
        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_CLEARING_DLC_QUEUE + dlcQueueName);
            throw rdbmsStoreUtils.convertSQLException("error occurred while clearing dlc queue:" +
                    dlcQueueName, e);
        } finally {
            contextWrite.stop();
            close(connection, preparedStatement, RDBMSConstants.TASK_CLEARING_DLC_QUEUE + dlcQueueName);
        }
        return deletedMessagecount;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Long> getMessageIDsAddressedToQueue(String storageQueueName, Long startMessageID)
            throws AndesException {

        List<Long> messageIDs = new ArrayList<>();

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet results = null;

        Context contextRead = MetricManager.timer(Level.INFO, MetricsConstants.DB_READ).start();

        try {
            connection = getConnection();

            preparedStatement = connection
                    .prepareStatement(RDBMSConstants.PS_SELECT_MESSAGE_IDS_FROM_METADATA_FOR_QUEUE);
            preparedStatement.setInt(1, getCachedQueueID(storageQueueName));

            results = preparedStatement.executeQuery();

            while (results.next()) {
                messageIDs.add(results.getLong(RDBMSConstants.MESSAGE_ID));
            }

        } catch (SQLException e) {
            throw rdbmsStoreUtils.convertSQLException("Error while getting message IDs for queue : " +
                    storageQueueName, e);
        } finally {
            contextRead.stop();
            close(connection, preparedStatement, results,
                  RDBMSConstants.TASK_RETRIEVING_NEXT_N_MESSAGE_IDS_OF_QUEUE + storageQueueName);
        }

        return messageIDs;

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addQueue(String destinationQueueName) throws AndesException {
        Connection connection = null;
        try {
            connection = getConnection();
            getCachedQueueID(destinationQueueName);
        } catch (SQLException e) {
            throw rdbmsStoreUtils.convertSQLException("Error while creating queue: " + destinationQueueName, e);
        } finally {
            close(connection, RDBMSConstants.TASK_CREATING_QUEUE + destinationQueueName);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Integer> getMessageCountForAllQueues(List<String> queueNames) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet results = null;
        Map<String, Integer> queueMessageCountForName = new HashMap<>();
        Context contextRead = MetricManager.timer(Level.INFO, MetricsConstants.DB_READ).start();
        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_SELECT_ALL_QUEUE_MESSAGE_COUNT);
            results = preparedStatement.executeQuery();

            // Each row in the result gives the queue name and the number of messages remaining. All these rows are
            // added to a map
            // Dead letter channel queues are not retrieved by the operation. Therefore we need to skip it
            // Also if the number of messages in the result set is null it means that there are no messages left in the
            // db for that queue. Hence we add the value 0 for those queue
            while (results.next()) {
                String queueName = results.getString(RDBMSConstants.QUEUE_NAME);
                if (!(DLCQueueUtils.isDeadLetterQueue(queueName)) && queueNames.contains(queueName)){
                    queueMessageCountForName.put(queueName,results.getInt(RDBMSConstants.PS_ALIAS_FOR_COUNT));
                }
            }

        } catch (SQLException e) {
            throw rdbmsStoreUtils.convertSQLException("Error while getting message count for all queues", e);
        } finally {
            contextRead.stop();
            close(connection, preparedStatement, results, RDBMSConstants.TASK_RETRIEVING_QUEUE_MSG_COUNT);
        }
        return queueMessageCountForName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getMessageCountForQueue(String storageQueueName) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet results = null;
        long messageCount = 0;
        Context contextRead = MetricManager.timer(Level.INFO, MetricsConstants.DB_READ).start();
        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_SELECT_QUEUE_MESSAGE_COUNT);
            preparedStatement.setInt(1, getCachedQueueID(storageQueueName));

            results = preparedStatement.executeQuery();

            while (results.next()) {
                messageCount = results.getLong(RDBMSConstants.PS_ALIAS_FOR_COUNT);
            }

        } catch (SQLException e) {
            throw rdbmsStoreUtils.convertSQLException("Error while getting message count from queue " +
                    storageQueueName, e);
        } finally {
            contextRead.stop();
            close(connection, preparedStatement, results,
                  RDBMSConstants.TASK_RETRIEVING_QUEUE_MSG_COUNT + storageQueueName);
        }

        return messageCount;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getMessageCountForQueueInDLC(String storageQueueName, String dlcQueueName) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet results = null;
        long messageCount = 0;
        Context contextRead = MetricManager.timer(Level.INFO, MetricsConstants.DB_READ).start();
        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_SELECT_QUEUE_MESSAGE_COUNT_FROM_DLC);
            preparedStatement.setInt(1, getCachedQueueID(storageQueueName));
            preparedStatement.setInt(2, getCachedQueueID(dlcQueueName));
            results = preparedStatement.executeQuery();

            while (results.next()) {
                messageCount = results.getLong(RDBMSConstants.PS_ALIAS_FOR_COUNT);
            }

        } catch (SQLException e) {
            throw rdbmsStoreUtils.convertSQLException("Error while getting message count in DLC from queue " +
                    storageQueueName, e);
        } finally {
            contextRead.stop();
            close(connection, preparedStatement, results,
                  RDBMSConstants.TASK_RETRIEVING_QUEUE_MSG_COUNT_IN_DLC + storageQueueName);
        }
        return messageCount;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getMessageCountForDLCQueue(String dlcQueueName) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet results = null;
        long messageCount = 0;
        Context contextRead = MetricManager.timer(Level.INFO, MetricsConstants.DB_READ).start();
        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_SELECT_MESSAGE_COUNT_IN_DLC);
            preparedStatement.setInt(1, getCachedQueueID(dlcQueueName));
            results = preparedStatement.executeQuery();

            while (results.next()) {
                messageCount = results.getLong(RDBMSConstants.PS_ALIAS_FOR_COUNT);
            }

        } catch (SQLException e) {
            throw rdbmsStoreUtils.convertSQLException("Error while getting message count in DLC from queue ", e);
        } finally {
            contextRead.stop();
            close(connection, preparedStatement, results, RDBMSConstants.TASK_RETRIEVING_QUEUE_MSG_COUNT_IN_DLC);
        }
        return messageCount;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void resetMessageCounterForQueue(String storageQueueName) throws AndesException {
        // Message count is taken from DB itself. No need to implement this
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeQueue(String storageQueueName) throws AndesException {
        queueMap.remove(storageQueueName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void incrementMessageCountForQueue(String destinationQueueName, long incrementBy) throws AndesException {
        // Message count is taken from DB itself. No need to implement this
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void decrementMessageCountForQueue(String destinationQueueName, long decrementBy) throws AndesException {
        // Message count is taken from DB itself. No need to implement this
    }

    /**
     * Store retained messages in RDBMS message stores
     *
     * {@inheritDoc}
     */
    @Override
    public void storeRetainedMessages(Map<String,AndesMessage> retainMap) throws AndesException {

        Connection connection = null;

        PreparedStatement updateMetadataPreparedStatement = null;
        PreparedStatement deleteContentPreparedStatement = null;
        PreparedStatement deleteMetadataPreparedStatement = null;
        PreparedStatement insertContentPreparedStatement = null;
        Context contextWrite = MetricManager.timer(Level.INFO, MetricsConstants.DB_WRITE).start();

        boolean batchEmpty = true;

        try {
            connection = getConnection();

            updateMetadataPreparedStatement = connection.prepareStatement(
                    RDBMSConstants.PS_UPDATE_RETAINED_METADATA);
            deleteContentPreparedStatement = connection.prepareStatement(
                    RDBMSConstants.PS_DELETE_RETAIN_MESSAGE_PARTS);
            deleteMetadataPreparedStatement = connection.prepareStatement(
                    RDBMSConstants.PS_DELETE_RETAIN_MESSAGE_METADATA);
            insertContentPreparedStatement = connection.prepareStatement(
                    RDBMSConstants.PS_INSERT_RETAIN_MESSAGE_PART);
            for (AndesMessage message : retainMap.values()) {

                AndesMessageMetadata metadata = message.getMetadata();
                String destination = metadata.getDestination();
                RetainedItemData retainedItemData = getRetainedTopicID(connection, destination);

                if (null != retainedItemData) {

                    if (batchEmpty) {
                        batchEmpty = false;
                    }

                    addRetainedMessageToUpdateBatch(updateMetadataPreparedStatement,
                                                    deleteContentPreparedStatement,
                                                    deleteMetadataPreparedStatement,
                                                    insertContentPreparedStatement,
                                                    message, metadata, retainedItemData);
                    retainedItemData.messageID = metadata.getMessageID();

                } else {
                    // Retain message shouldn't create a retain entry if it receives an empty payload.
                    if(!message.getContentChunkList().isEmpty()) {
                        // if first chunk doesn't contain any data that payload is empty. Therefore
                        // no need to store the retain message.
                        if (message.getContentChunkList().get(0).getDataLength() != 0) {
                            createRetainedEntry(connection, message);
                        }
                    }
                }

            }

            if (!batchEmpty) {
                deleteContentPreparedStatement.executeBatch();
                deleteMetadataPreparedStatement.executeBatch();
                updateMetadataPreparedStatement.executeBatch();
                insertContentPreparedStatement.executeBatch();
                connection.commit();
            }

        } catch (SQLException e) {
            rollback(connection, RDBMSConstants.TASK_STORING_RETAINED_MESSAGE);
            throw rdbmsStoreUtils.convertSQLException("Error occurred while adding retained message content to DB ", e);

        } finally {
            contextWrite.stop();
            close(updateMetadataPreparedStatement, RDBMSConstants.TASK_STORING_RETAINED_MESSAGE);
            close(deleteContentPreparedStatement, RDBMSConstants.TASK_STORING_RETAINED_MESSAGE);
            close(deleteMetadataPreparedStatement, RDBMSConstants.TASK_STORING_RETAINED_MESSAGE);
            close(insertContentPreparedStatement, RDBMSConstants.TASK_STORING_RETAINED_MESSAGE);
            close(connection, RDBMSConstants.TASK_STORING_RETAINED_MESSAGE);
        }
    }


    /**
     * Used to store details about a retained item entry
     */
    private static class RetainedItemData {
        /**
         * Topic id of the DB entry
         */
        public int topicID;

        /**
         * Retained message ID for the topic
         */
        public long messageID;

        private RetainedItemData(Integer topicID, long messageID) {
            this.topicID = topicID;
            this.messageID = messageID;
        }
    }

    /**
     * Update batching prepared statements with current message.
     *
     * @param updateMetadataPreparedStatement update prepared statement
     * @param deleteContentPreparedStatement  delete content prepared statement
     * @param deleteMetadataPreparedStatement delete metadata prepared statement
     * @param insertContentPreparedStatement  insert prepared statement
     * @param message                         current message
     * @param metadata                        current message metadata
     * @param retainedItemData                retained item data
     * @throws SQLException
     */
    private void addRetainedMessageToUpdateBatch(PreparedStatement updateMetadataPreparedStatement,
                                                 PreparedStatement deleteContentPreparedStatement,
                                                 PreparedStatement deleteMetadataPreparedStatement,
                                                 PreparedStatement insertContentPreparedStatement,
                                                 AndesMessage message,
                                                 AndesMessageMetadata metadata,
                                                 RetainedItemData retainedItemData)
            throws SQLException {

        // Will set to true if payload of retained message is empty. Therefore, instead of update
        // retain topic entry will be deleted from database.
        boolean isRetainTopicSetToDelete = false;


        // If retain topic message received with empty payload that particular retain topic will be
        // deleted from broker instead of updating.
        if(!message.getContentChunkList().isEmpty()) {
            // if first chunk doesn't contain any data that payload is empty and therefore
            // retain message(metadata + content) should remove.
            if (message.getContentChunkList().get(0).getDataLength() == 0) {

                isRetainTopicSetToDelete = true;

                deleteMetadataPreparedStatement.setLong(1, retainedItemData.messageID);
                deleteMetadataPreparedStatement.addBatch();

                deleteContentPreparedStatement.setLong(1, retainedItemData.messageID);
                deleteContentPreparedStatement.addBatch();

            }
        }

        // If retain topic is not set to delete (payload is not empty) following 3 operations will occur.
        // 1. metadata will be updated with new parameters.
        // 2. old content corresponding to previous retain message will be deleted.
        // 3. new content will be created for newly arrived retained message.
        if(!isRetainTopicSetToDelete) {

            // update metadata
            updateMetadataPreparedStatement.setLong(1, metadata.getMessageID());
            updateMetadataPreparedStatement.setBytes(2, metadata.getMetadata());
            updateMetadataPreparedStatement.setInt(3, retainedItemData.topicID);
            updateMetadataPreparedStatement.addBatch();

            // update content
            deleteContentPreparedStatement.setLong(1, retainedItemData.messageID);
            deleteContentPreparedStatement.addBatch();
            for (AndesMessagePart messagePart : message.getContentChunkList()) {
                insertContentPreparedStatement.setLong(1, metadata.getMessageID());
                insertContentPreparedStatement.setInt(2, messagePart.getOffset());
                insertContentPreparedStatement.setBytes(3, messagePart.getData());
                insertContentPreparedStatement.addBatch();
            }
        }
    }

    /**
     * Get retained topic ID for destination.
     *
     * @param connection  database connection to used
     * @param destination destination name
     * @return Retained item data
     * @throws SQLException
     */
    private RetainedItemData getRetainedTopicID(Connection connection, String destination)
            throws SQLException {
        PreparedStatement preparedStatementForMetadataSelect = null;
        RetainedItemData itemData = null;
        Context contextRead = MetricManager.timer(Level.INFO, MetricsConstants.DB_READ).start();
        try {
            preparedStatementForMetadataSelect = connection
                    .prepareStatement(RDBMSConstants.PS_SELECT_RETAINED_MESSAGE_ID);
            preparedStatementForMetadataSelect.setString(1, destination);
            ResultSet results = preparedStatementForMetadataSelect.executeQuery();

            if (results.next()) {
                int topicID = results.getInt(RDBMSConstants.TOPIC_ID);
                long messageID = results.getLong(RDBMSConstants.MESSAGE_ID);
                itemData = new RetainedItemData(topicID, messageID);
            }
        } finally {
            contextRead.stop();
            close(preparedStatementForMetadataSelect, RDBMSConstants.TASK_RETRIEVING_RETAINED_TOPIC_ID);
        }

        return itemData;
    }

    /**
     * Create a new entry for retained message.
     *
     * @param connection database connection
     * @param message    retained message
     * @return Retained item data
     * @throws SQLException
     */
    private RetainedItemData createRetainedEntry(Connection connection, AndesMessage message) throws SQLException {
        PreparedStatement preparedStatementForContent = null;
        PreparedStatement preparedStatementForMetadata = null;

        AndesMessageMetadata metadata = message.getMetadata();
        String destination = metadata.getDestination();
        Integer topicID = destination.hashCode();
        long messageID = metadata.getMessageID();
        Context contextWrite = MetricManager.timer(Level.INFO, MetricsConstants.DB_WRITE).start();
        try {
            // create metadata entry
            preparedStatementForMetadata = connection.prepareStatement(
                    RDBMSConstants.PS_INSERT_RETAINED_METADATA);
            preparedStatementForMetadata.setInt(1, topicID);
            preparedStatementForMetadata.setString(2, destination);
            preparedStatementForMetadata.setLong(3, messageID);
            preparedStatementForMetadata.setBytes(4, metadata.getMetadata());
            preparedStatementForMetadata.addBatch();

            // create content
            preparedStatementForContent = connection.prepareStatement(
                    RDBMSConstants.PS_INSERT_RETAIN_MESSAGE_PART);
            for (AndesMessagePart messagePart : message.getContentChunkList()) {
                preparedStatementForContent.setLong(1, messageID);
                preparedStatementForContent.setInt(2, messagePart.getOffset());
                preparedStatementForContent.setBytes(3, messagePart.getData());
                preparedStatementForContent.addBatch();
            }

            preparedStatementForMetadata.executeBatch();
            preparedStatementForContent.executeBatch();
            connection.commit();

            return new RetainedItemData(topicID, messageID);
        } finally {
            contextWrite.stop();
            close(preparedStatementForContent, RDBMSConstants.TASK_STORING_RETAINED_MESSAGE);
            close(preparedStatementForMetadata, RDBMSConstants.TASK_STORING_RETAINED_MESSAGE);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getAllRetainedTopics() throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatementForTopicSelect = null;
        List<String> topicList = new ArrayList<>();
        ResultSet results = null;
        Context contextRead = MetricManager.timer(Level.INFO, MetricsConstants.DB_READ).start();
        try {
            connection = getConnection();

            preparedStatementForTopicSelect = connection.prepareStatement(RDBMSConstants.PS_SELECT_ALL_RETAINED_TOPICS);
            results = preparedStatementForTopicSelect.executeQuery();

            while (results.next()) {
                topicList.add(results.getString(RDBMSConstants.TOPIC_NAME));
            }

        } catch (SQLException e) {
            throw rdbmsStoreUtils.convertSQLException("Error occurred while reading retained topics ", e);
        } finally {
            close(connection, preparedStatementForTopicSelect, results, RDBMSConstants.TASK_RETRIEVING_RETAINED_TOPICS);
            contextRead.stop();
        }

        return topicList;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public DeliverableAndesMetadata getRetainedMetadata(String destination) throws AndesException {
        DeliverableAndesMetadata metadata = null;
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet results = null;
        Context contextRead = MetricManager.timer(Level.INFO, MetricsConstants.DB_READ).start();

        try {

            connection = getConnection();

            RetainedItemData retainedItemData = getRetainedTopicID(connection, destination);

            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_SELECT_RETAINED_METADATA);
            preparedStatement.setLong(1, retainedItemData.topicID);

            results = preparedStatement.executeQuery();

            if (results.next()) {
                byte[] b = results.getBytes(RDBMSConstants.METADATA);
                long messageId = results.getLong(RDBMSConstants.MESSAGE_ID);
                metadata = new DeliverableAndesMetadata(null, messageId, b, true);
            }
        } catch (SQLException e) {
            throw rdbmsStoreUtils.convertSQLException("error occurred while retrieving retained message " +
                    "for destination:" + destination, e);
        } finally {
            contextRead.stop();
            close(connection, preparedStatement, results, "Retrieve retained message for destination");
        }
        return metadata;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<Integer, AndesMessagePart> getRetainedContentParts(long messageID) throws AndesException {

        Connection connection = null;

        PreparedStatement preparedStatement = null;

        ResultSet results = null;
        Map<Integer, AndesMessagePart> contentParts = new HashMap<>();

        Context contextRead = MetricManager.timer(Level.INFO, MetricsConstants.DB_READ).start();

        try {
            connection = getConnection();

            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_RETRIEVE_RETAIN_MESSAGE_PART);
            preparedStatement.setLong(1, messageID);
            results = preparedStatement.executeQuery();

            while (results.next()) {
                byte[] b = results.getBytes(RDBMSConstants.MESSAGE_CONTENT);
                int offset = results.getInt(RDBMSConstants.MSG_OFFSET);

                AndesMessagePart messagePart = new AndesMessagePart();

                messagePart.setMessageID(messageID);
                messagePart.setData(b);
                messagePart.setDataLength(b.length);
                messagePart.setOffSet(offset);
                contentParts.put(offset, messagePart);
            }
        } catch (SQLException e) {
            throw rdbmsStoreUtils.convertSQLException("Error occurred while retrieving retained message content from DB" +
                    " [msg_id=" + messageID + "]", e);
        } finally {
            contextRead.stop();
            close(connection, preparedStatement, results, RDBMSConstants.TASK_RETRIEVING_RETAINED_MESSAGE_PARTS);
        }
        return contentParts;
    }

    /**
     * {@inheritDoc} Check if data can be inserted, read and finally deleted
     * from the database.
     */
    public boolean isOperational(String testString, long testTime) {

        try {
            // Here order is important
            return rdbmsStoreUtils.testInsert(getConnection(), testString, testTime) &&
                    rdbmsStoreUtils.testRead(getConnection(), testString, testTime) &&
                    rdbmsStoreUtils.testDelete(getConnection(), testString, testTime);
        } catch (SQLException e) {
            return false;
        }
    }
    
    
    /**
     * Add thie given message to cache
     * 
     * @param message
     *            the message
     */
    private void addToCache(AndesMessage message) {
        messageCache.addToCache(message);
    }

    /**
     * Removes given list of messages/ids from the cache
     * 
     * @param messagesToRemove
     *            list of messages
     */
    private void removeFromCache(List<Long> messagesToRemove) {
        messageCache.removeFromCache(messagesToRemove);
    }

    /**
     * Removes a message with a given Id from the cache.
     *
     * @param messageToRemove message Id of the message to be removed
     */
    private void removeFromCache(long messageToRemove) {
        messageCache.removeFromCache(messageToRemove);
    }

    /**
     * Returns a message if found in cache.
     * 
     * @param messageId
     *            message id to look up
     * @return a message or null (if not found)
     */
    private AndesMessage getMessageFromCache(long messageId) {
        return messageCache.getMessageFromCache(messageId);
    }

    /**
     * Get the list of messages found from the cache.
     * <b> This method modifies the provided messageIDList </b>
     * 
     * @param messageIDList
     *            message id to be found in cache.
     * @param contentList
     *            the list the fill
     */
    private void fillContentFromCache(List<Long> messageIDList, Map<Long, List<AndesMessagePart>> contentList) {
        messageCache.fillContentFromCache(messageIDList, contentList);
    }

    /**
     * Return a {@link AndesMessagePart} from the cache.
     * 
     * @param messageId
     *            id of the massage
     * @param offsetValue
     *            the offset value
     * @return a {@link AndesMessagePart} if the message is found otherwise null
     */
    private AndesMessagePart getContentFromCache(long messageId, int offsetValue) {
        return messageCache.getContentFromCache(messageId, offsetValue);
    }
    
}

