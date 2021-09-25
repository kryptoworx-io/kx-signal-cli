package io.kryptoworx.signalcli.storage;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

public class NonCloseableOutputStream extends OutputStream {
	private final OutputStream delegate;

	public NonCloseableOutputStream(OutputStream delegate) {
		this.delegate = delegate;
	}

	public int hashCode() {
		return delegate.hashCode();
	}

	public void write(int b) throws IOException {
		delegate.write(b);
	}

	public void write(byte[] b) throws IOException {
		delegate.write(b);
	}

	public boolean equals(Object obj) {
		return delegate.equals(obj);
	}

	public void write(byte[] b, int off, int len) throws IOException {
		delegate.write(b, off, len);
	}

	public void flush() throws IOException {
		delegate.flush();
	}

	public void close() throws IOException {

	}

	public String toString() {
		return delegate.toString();
	}
	
	public static NonCloseableOutputStream create(WritableByteChannel channel) {
		return new NonCloseableOutputStream(Channels.newOutputStream(channel));
	}
}