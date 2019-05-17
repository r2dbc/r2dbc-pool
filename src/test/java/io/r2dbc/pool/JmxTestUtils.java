package io.r2dbc.pool;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * Test utility class for JMX related operations.
 *
 * @author Tadaya Tsuyukubo
 */
class JmxTestUtils {

    private JmxTestUtils() {
    }

    public static List<ObjectName> getPoolMBeanNames() {

        MBeanServer mBeanServer = getMBeanServer();

        return mBeanServer.queryMBeans(null, null)
            .stream()
            .map(ObjectInstance::getObjectName)
            .filter(objectName -> ConnectionPoolMXBean.DOMAIN.equals(objectName.getDomain()))
            .collect(toList());
    }

    public static void unregisterPoolMbeans() {
        MBeanServer mBeanServer = getMBeanServer();
        getPoolMBeanNames().forEach(objectName -> {
            try {
                mBeanServer.unregisterMBean(objectName);
            } catch (JMException e) {
            }
        });
    }

    private static MBeanServer getMBeanServer() {
        return ManagementFactory.getPlatformMBeanServer();
    }
}
