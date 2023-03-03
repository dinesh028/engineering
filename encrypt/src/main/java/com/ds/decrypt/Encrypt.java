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
import java.security.SecureRandom;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.naming.OperationNotSupportedException;

import org.apache.commons.io.FileUtils;
import org.bouncycastle.crypto.engines.AESEngine;
import org.bouncycastle.crypto.generators.OpenSSLPBEParametersGenerator;
import org.bouncycastle.crypto.io.CipherOutputStream;
import org.bouncycastle.crypto.modes.CBCBlockCipher;
import org.bouncycastle.crypto.paddings.PaddedBufferedBlockCipher;
import org.bouncycastle.crypto.params.ParametersWithIV;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;

public class Encrypt {

	public static byte[] toBytes(int... xs) {
		byte[] retArr = new byte[xs.length];

		int j = 0;
		for (int i : xs) {
			retArr[j] = new Integer(i).byteValue();
		}
		return retArr;

	}

	public static void encrypt(String algorithm, String key, String inFile, String outFile)
			throws OperationNotSupportedException, IOException, IllegalBlockSizeException, BadPaddingException,
			InvalidKeySpecException, NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException {
		switch (algorithm) {
		case "AES-256":
		case "AES-128": {
			// Generate random 8 bytes salt
			SecureRandom random = new SecureRandom();
			byte[] salt = new byte[8];
			random.nextBytes(salt);

			byte[] password = key.getBytes(StandardCharsets.UTF_8);
			// SHA256 as of v1.1.0 (if in OpenSSL the default digest is applied)
			OpenSSLPBEParametersGenerator pbeGenerator = new OpenSSLPBEParametersGenerator();
			pbeGenerator.init(password, salt);

			// Default 256
			ParametersWithIV parameters = (ParametersWithIV) pbeGenerator.generateDerivedParameters(256, 128);

			if (algorithm.equals("AES-128")) {
				parameters = (ParametersWithIV) pbeGenerator.generateDerivedParameters(128, 128);
			}

			// Encrypt with AES-256, CBC using streams
			FileOutputStream fos = new FileOutputStream(outFile);
			// Apply OpenSSL format
			fos.write("Salted__".getBytes(StandardCharsets.UTF_8));
			fos.write(salt);

			PaddedBufferedBlockCipher cipher = new PaddedBufferedBlockCipher(new CBCBlockCipher(new AESEngine()));
			cipher.init(true, parameters);
			FileInputStream fis = null;
			CipherOutputStream cos = null;
			try {
				fis = new FileInputStream(inFile);
				cos = new CipherOutputStream(fos, cipher);
				int bytesRead = -1;

				byte[] buffer = toBytes(64, 1024, 1024);

				while (true) {
					bytesRead = fis.read(buffer);
					if (bytesRead == -1) {
						break;
					}
					cos.write(buffer, 0, bytesRead);
				}

			} finally {
				if (cos != null && fis != null) {
					cos.flush();
					cos.close();
					fis.close();
				}
			}

			break;
		}
		case "RSAES-OAEP": {
			//Read the Public Key
			StringReader reader = new StringReader(key);
			PemReader pubpemReader = new PemReader(reader);
	        PemObject pubpemObject = pubpemReader.readPemObject();
	        byte[] content = pubpemObject.getContent();
	        X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec(content);

	        // Generate public key from pem file
	        KeyFactory factory = KeyFactory.getInstance("RSA");
	        RSAPublicKey rsaPublicKey = (RSAPublicKey)factory.generatePublic(pubKeySpec);

	        // Data to be Encryted
	        byte[] inDataContent = FileUtils.readFileToString(new File(inFile), "UTF-8").getBytes(StandardCharsets.UTF_8);

	        //Encrypt Data with RSA
	        Cipher pipher = Cipher.getInstance("RSA");
	        pipher.init(Cipher.ENCRYPT_MODE, rsaPublicKey);
	        byte[] encryptedAESContent = pipher.doFinal(inDataContent);

	        // Write encrypted details to file
	        Files.write(Paths.get(outFile) , encryptedAESContent);
			break;
		}
		default: {
			throw new OperationNotSupportedException(algorithm + " not supported");
		}
		}
	}
}
