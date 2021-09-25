package io.kryptoworx.signalcli.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.UUID;
import java.util.function.Consumer;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.Mac;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.bouncycastle.util.Arrays;

public class CipherStreamFactory implements AutoCloseable {
	private static final String HMAC = "HmacSHA512/256";
	private static final String CIPHER = "ChaCha20-Poly1305";
	private static final int NONCE_LENGTH = 12;

    private final byte[] factoryKey;
    private final SecureRandom prng = new SecureRandom();

    public CipherStreamFactory(UUID id, byte[] masterKey) {
        this.factoryKey = createFactoryKey(id, masterKey);
    }
    
    private static byte[] createFactoryKey(UUID id, byte[] masterKey) {
    	return deriveKey(16, 
    			buf -> {
		    		buf.putLong(id.getMostSignificantBits());
		    		buf.putLong(id.getLeastSignificantBits());
		    	},
    			masterKey);
    }

    private byte[] createFileKey(String fileName, byte[] nonce) {
		if (nonce == null || nonce.length != NONCE_LENGTH) {
			throw new IllegalArgumentException();
		}
		CharBuffer fileNameBuf = CharBuffer.wrap(fileName);
		return deriveKey(fileNameBuf.length() * 4  + nonce.length,
				buf -> {
					StandardCharsets.UTF_8.newEncoder().encode(fileNameBuf, buf, true);
			    	buf.put(nonce);
				},
				factoryKey);
	}
    
    private static byte[] deriveKey(int infoByteCount, Consumer<ByteBuffer> infoConsumer, byte[] masterKey) {
		if (masterKey == null || masterKey.length != 32) {
			throw new IllegalArgumentException();
		}
		try {
			SecretKeySpec secretKey = new SecretKeySpec(masterKey, HMAC);
			Mac mac = Mac.getInstance(HMAC);
			mac.init(secretKey);
			ByteBuffer macInput = ByteBuffer.allocate(infoByteCount + 1);
			infoConsumer.accept(macInput);
			macInput.put((byte) 1);
			macInput.flip();
			mac.update(macInput);
			return mac.doFinal();
		} catch (GeneralSecurityException e) {
			throw new AssertionError("Key derivation failed", e);
		}
	}


    public CipherOutputStream createOutputStream(File file) throws IOException {
    	return createOutputStream(file.getName(), new FileOutputStream(file));
    }
    
    public CipherOutputStream createOutputStream(String fileName, OutputStream out) throws IOException {
    	Cipher cipher = createCipher();
    	byte[] nonce = new byte[NONCE_LENGTH];
    	prng.nextBytes(nonce);
    	byte[] fileKey = createFileKey(fileName, nonce);
    	initCipher(cipher, Cipher.ENCRYPT_MODE, fileKey, nonce);
    	clear(fileKey);
    	out.write(nonce);
    	out.flush();
    	return new CipherOutputStream(out, cipher);
    }
    
    private static Cipher createCipher() {
    	try {
    		return Cipher.getInstance(CIPHER);
    	} catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
    		throw new AssertionError("Failed create cipher", e);
    	}
    }
    
    private static void initCipher(Cipher cipher, int opmode, byte[] keyBytes, byte[] nonceBytes) {
    	IvParameterSpec ivParameter = new IvParameterSpec(nonceBytes); 
    	SecretKey key = new SecretKeySpec(keyBytes, "ChaCha20");
    	try {
			cipher.init(opmode, key, ivParameter);
		} catch (InvalidKeyException | InvalidAlgorithmParameterException e) {
			throw new AssertionError("Failed to initialize cipher", e);
		}
    }

    public CipherInputStream createInputStream(File file) throws IOException {
    	return createInputStream(file.getName(), new FileInputStream(file));
    }
    
    public CipherInputStream createInputStream(String fileName, InputStream in) throws IOException {
    	byte[] nonce = in.readNBytes(NONCE_LENGTH);
    	if (nonce.length != NONCE_LENGTH) {
    		in.close();
    		throw new IOException("Illegal file content");
    	}
    	Cipher cipher = createCipher();
    	byte[] fileKey = createFileKey(fileName, nonce);
    	initCipher(cipher, Cipher.DECRYPT_MODE, fileKey, nonce);
    	clear(fileKey);
    	return new CipherInputStream(in, cipher);
    }

	@Override
	public void close() {
		clear(factoryKey);
	}
	
	private static void clear(byte[] buf) {
		if (buf != null) Arrays.fill(buf, (byte) 0);
	}
}
