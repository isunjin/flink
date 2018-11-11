package org.apache.flink.kubernetes.cli;

import org.apache.flink.util.FlinkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

public class PropertyUtil {

	private static final Logger LOG = LoggerFactory.getLogger(PropertyUtil.class);

	private File propertyFile;

	public PropertyUtil(String filePath) {
		this.propertyFile = new File(filePath);
	}

	public void delete() {
		// try to clean up the old yarn properties file
		try {
			if (this.propertyFile.isFile()) {
				if (this.propertyFile.delete()) {
					LOG.info("Deleted properties file at {}", this.propertyFile.getAbsoluteFile());
				} else {
					LOG.warn("Couldn't delete properties file at {}", this.propertyFile.getAbsoluteFile());
				}
			}
		} catch (Exception e) {
			LOG.warn("Exception while deleting property file", e);
		}
	}

	public void write(Properties properties) {
		try (final OutputStream out = new FileOutputStream(this.propertyFile)) {
			properties.store(out, "Generated properties file");
		} catch (IOException e) {
			throw new RuntimeException("Error writing the properties file", e);
		}
		this.propertyFile.setReadable(true, false); // readable for all.
	}

	public Properties read() throws FlinkException {
		Properties properties = new Properties();
		try (InputStream is = new FileInputStream(this.propertyFile)) {
			properties.load(is);
		} catch (IOException ioe) {
			LOG.info("Could not read properties file " + this.propertyFile.getAbsoluteFile());
		}
		return properties;
	}
}
