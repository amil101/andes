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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel;

import io.netty.buffer.ByteBuf;
import org.apache.log4j.Logger;

import java.util.Map;

import static io.netty.buffer.Unpooled.wrappedBuffer;

/**
 * DisruptorCachedContent has access to content cache built by the disruptor
 */

public class DisruptorCachedContent implements AndesContent {
    /**
     * Used to store message parts in memory
     */
    private final Map<Integer, AndesMessagePart> contentList;
    /**
     * Content length of the message
     */
    private final int contentLength;
    private static final Logger log = Logger.getLogger(DisruptorCachedContent.class);

    /**
     * Maximum chunk size allowed within Andes core
     */
    private final int maxChunkSize;

    /**
     * Create a {@link org.wso2.andes.kernel.DisruptorCachedContent} object
     * @param contentList message part list
     * @param contentLength length of the content to be cached
     * @param maxChunkSize maximum chunk size of the stored content
     */
    public DisruptorCachedContent(Map<Integer, AndesMessagePart> contentList, int contentLength, int maxChunkSize) {
        this.contentList = contentList;
        this.contentLength = contentLength;
        this.maxChunkSize = maxChunkSize;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int putContent(int offset, ByteBuf destinationBuffer) throws AndesException {
        int written = 0;
//        int remainingBufferSpace = destinationBuffer.remaining();
        int remainingBufferSpace = destinationBuffer.writableBytes();
        int remainingContent = contentLength - offset;
        int maxRemaining = Math.min(remainingBufferSpace, remainingContent);

        int currentBytePosition = offset;

        while (maxRemaining > written) {
            // This is an integer division
            int chunkNumber = currentBytePosition / maxChunkSize;
            int chunkStartByteIndex = chunkNumber * maxChunkSize;
            int positionToReadFromChunk = currentBytePosition - chunkStartByteIndex;

            AndesMessagePart messagePart = getMessagePart(chunkStartByteIndex);

            int messagePartSize = messagePart.getDataLength();
            int numOfBytesAvailableToRead = messagePartSize - positionToReadFromChunk;
            int remaining = maxRemaining - written;
            int numOfBytesToRead;

            if (remaining > numOfBytesAvailableToRead) {
                numOfBytesToRead = numOfBytesAvailableToRead;
            } else {
                numOfBytesToRead = remaining;
            }

//            destinationBuffer.put(messagePart.getData(), positionToReadFromChunk, numOfBytesToRead);
            messagePart.getData().clear();
            ByteBuf slice=wrappedBuffer(messagePart.getData()).slice(positionToReadFromChunk,numOfBytesToRead);
            slice.clear();
            destinationBuffer.writeBytes(slice,0,slice.capacity());
            written = written + numOfBytesToRead;
            currentBytePosition = currentBytePosition + numOfBytesToRead;

        }

        return written;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getContentLength() {
        return contentLength;
    }

    /**
     * Get Message part for byte index
     *
     * @param indexToQuery
     *         Byte index of the content
     * @return Content chunk
     */
    private AndesMessagePart getMessagePart(int indexToQuery) throws AndesException {
        AndesMessagePart messagePart = contentList.get(indexToQuery);

        if (null == messagePart) {
            throw new AndesException("Content not cached for chunk index " + indexToQuery);
        }
        return messagePart;
    }
}
