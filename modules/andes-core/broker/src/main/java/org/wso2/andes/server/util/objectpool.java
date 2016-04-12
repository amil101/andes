package org.wso2.andes.server.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.log4j.Logger;

/**
 * Created by amil101 on 2/24/16.
 */
public class objectpool {

    private static final Logger log = Logger.getLogger(objectpool.class);
    private ByteBufAllocator alloc = null;
    private static objectpool instance = new objectpool();

    private objectpool() {
    }

    public static objectpool getInstance() {
        return instance;
    }

    public ByteBuf setDirectMemory(int payloadSize) {
        if (alloc == null) {
            alloc = PooledByteBufAllocator.DEFAULT;
            log.warn("object pool memory allocated successfully!");
        }
        ByteBuf bb = alloc.directBuffer(payloadSize);
        return bb;

    }
}
