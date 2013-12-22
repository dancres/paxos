package org.dancres.paxos.test.utils;

import java.io.File;

public class FileSystem {
	public static boolean deleteDirectory(File path) {
		if (path.exists()) {
			File[] files = path.listFiles();
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
		}
		return (path.delete());
	}
}
