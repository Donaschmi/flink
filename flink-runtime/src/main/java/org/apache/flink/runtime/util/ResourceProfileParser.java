package org.apache.flink.runtime.util;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;

public class ResourceProfileParser {
    public static ResourceProfile parseResourceProfile(String s) {
        String delim = "[{}]+";
        String[] strings = s.split(delim);
        if (strings.length <= 0 || !strings[0].equals(ResourceProfile.class.getSimpleName())) {
            return ResourceProfile.UNKNOWN;
        } else if (strings.length == 2) {
            if (strings[1].contains("UNKNOWN")) {
                return ResourceProfile.UNKNOWN;
            }
            String[] profiles = strings[1].split(", ");
            ResourceProfile.Builder resourceProfile = ResourceProfile.newBuilder()
                    .setCpuCores(Double.parseDouble(profiles[0].split("=")[1]))
                    .setTaskHeapMemoryMB(parseMemoryFromResourceProfile(profiles[1]))
                    .setTaskOffHeapMemoryMB(parseMemoryFromResourceProfile(profiles[2]))
                    .setManagedMemoryMB(parseMemoryFromResourceProfile(profiles[3]))
                    .setNetworkMemoryMB(parseMemoryFromResourceProfile(profiles[4]));
            return resourceProfile.build();

        } else {
            return ResourceProfile.UNKNOWN;
        }
    }

    private static int parseMemoryFromResourceProfile(String s) {
        String parsed = s.substring(s.indexOf("=") + 1, s.indexOf(" "));
        if (parsed.equals("0")) {
            return 0;
        }
        return Integer.parseInt(parsed.split("\\.")[0]);
    }
}
