package org.example.kafka;

import com.sun.management.OperatingSystemMXBean;

import java.lang.management.ManagementFactory;

/**
 * @author machenggong
 * @since 2021/4/19
 */
public class MemoryUsageExtrator {

    private static OperatingSystemMXBean mxBean =
            (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    /**
     * Get current free memory size in bytes
     *
     * @return free RAM size
     */
    public static long currentFreeMemorySizeInBytes() {
        OperatingSystemMXBean osmxb = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
        return osmxb.getFreePhysicalMemorySize();
    }

}
