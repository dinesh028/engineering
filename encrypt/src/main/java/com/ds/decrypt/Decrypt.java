package com.ds.decrypt;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;

import org.bouncycastle.crypto.io.CipherInputStream;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.naming.OperationNotSupportedException;

import org.apache.commons.io.FileUtils;
import org.bouncycastle.crypto.engines.AESEngine;
import org.bouncycastle.crypto.generators.OpenSSLPBEParametersGenerator;
import org.bouncycastle.crypto.modes.CBCBlockCipher;
import org.bouncycastle.crypto.paddings.PaddedBufferedBlockCipher;
import org.bouncycastle.crypto.params.ParametersWithIV;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;


public class Decrypt {

	public static byte[] toBytes(int... xs) {
		byte[] retArr = new byte[xs.length];

		int j = 0;
		for (int i : xs) {
			retArr[j] = new Integer(i).byteValue();
		}
		return retArr;

	}

	public static void decrypt(String algorithm, String key, String inFile, String outFile)
			throws OperationNotSupportedException, IOException, NoSuchAlgorithmException, InvalidKeySpecException, IllegalBlockSizeException, BadPaddingException, InvalidKeyException, NoSuchPaddingException {

		switch (algorithm) {
		case "AES-256":
		case "AES-128": {
			FileInputStream fis = null;
			CipherInputStream cis = null;
			FileOutputStream fos = null;
			try {

				// Read Input File
				fis = new FileInputStream(inFile);

				// Determine salt from OpenSSL format
				int i = 0;
				// First 8 for "Salted__" and 2nd 8 for Salt. Total 16
				byte[] firstBlock = new byte[16];
				while (i < firstBlock.length) {
					i += fis.read(firstBlock, i, firstBlock.length - i);
				}
				byte[] salt = java.util.Arrays.copyOfRange(firstBlock, 8, 16);

				// Derive 32 bytes key (AES_256) and 16 bytes IV
				byte[] password = key.getBytes(StandardCharsets.UTF_8);
				OpenSSLPBEParametersGenerator pbeGenerator = new OpenSSLPBEParametersGenerator();
				pbeGenerator.init(password, salt);

				// Default 256
				ParametersWithIV parameters = (ParametersWithIV) pbeGenerator.generateDerivedParameters(256, 128);
				// keySize, ivSize in bits

				if (algorithm.equals("AES-128")) {
					parameters = (ParametersWithIV) pbeGenerator.generateDerivedParameters(128, 128);
				}

				// Decrypt chunkwise (for large data)
				PaddedBufferedBlockCipher cipher = new PaddedBufferedBlockCipher(new CBCBlockCipher(new AESEngine()));
				cipher.init(false, parameters);
				cis = new CipherInputStream(fis, cipher);
				fos = new FileOutputStream(outFile);

				int bytesRead = -1;

				byte[] buffer = toBytes(64, 1024, 1024);

				while (true) {
					bytesRead = cis.read(buffer);
					if (bytesRead == -1) {
						break;
					}
					fos.write(buffer, 0, bytesRead);
				}

			} finally {
				// Closing input / output stream
				if (fis != null) {
					fis.close();
				}
				if (cis != null) {
					cis.close();
				}
				if (fos != null) {
					fos.close();
				}
			}

			break;
		}
		case "RSAES-OAEP": {
			String privateKey = FileUtils.readFileToString(new File(key), "UTF-8");
			StringReader reader = new StringReader(privateKey);
			PemReader privpemReader = new PemReader(reader);
			PemObject privpemObject = privpemReader.readPemObject();
			byte[] content = privpemObject.getContent();
			PKCS8EncodedKeySpec privKeySpec = new PKCS8EncodedKeySpec(content);
			
			 // Generate public key from pem file
			KeyFactory factory = KeyFactory.getInstance("RSA");
			RSAPrivateKey prvkey = (RSAPrivateKey) factory.generatePrivate(privKeySpec);
			
			// Data to be Encryted
	        byte[] inDataContent = Files.readAllBytes(Paths.get(inFile));

	        //Encrypt Data with RSA
	        Cipher pipher = Cipher.getInstance("RSA");
	        pipher.init(Cipher.DECRYPT_MODE, prvkey);
	        byte[] decryptedContent = pipher.doFinal(inDataContent);

	        // Write encrypted details to file
	        Files.write(Paths.get(outFile) , decryptedContent);
			
			
			break;
		}
		default: {
			throw new OperationNotSupportedException(algorithm + " not supported");
		}
		}
		System.out.println("Completed");
	}

	public static void main(String[] args) throws Exception {
		if (args == null || args.length != 4) {
			throw new IllegalArgumentException("Supported 4 args - algorithm, key, input_file, output_file");
		}

		decrypt(args[0], args[1], args[2], args[3]);
	}
}
