package com.link_intersystems.dbunit.testcontainers.consumer;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Locale;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class HashedCredentials {

    private static MessageDigest findMessageDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private final byte[] encodedhash;

    HashedCredentials(String username, String password) {
        username = username == null ? "" : username;
        password = password == null ? "" : password;
        char[] credentials = new char[username.length() + password.length()];

        char[] usernameChars = username.toCharArray();
        System.arraycopy(usernameChars, 0, credentials, 0, usernameChars.length);
        Arrays.fill(usernameChars, ' ');

        char[] passwordChars = password.toCharArray();
        System.arraycopy(passwordChars, 0, credentials, usernameChars.length, passwordChars.length);
        Arrays.fill(passwordChars, ' ');

        CharBuffer charBuffer = CharBuffer.wrap(credentials);
        ByteBuffer charsetEncodedCredentials = StandardCharsets.UTF_8.encode(charBuffer);
        Arrays.fill(credentials, ' ');

        byte[] charsetEncodedCredentialsArray = charsetEncodedCredentials.array();
        int remaining = charsetEncodedCredentials.remaining();

        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(charsetEncodedCredentialsArray, 0, remaining);
            encodedhash = digest.digest();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        Arrays.fill(charsetEncodedCredentialsArray, (byte) 0);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HashedCredentials that = (HashedCredentials) o;
        return Arrays.equals(encodedhash, that.encodedhash);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(encodedhash);
    }


    @Override
    public String toString() {
        StringBuilder hexString = new StringBuilder(2 * encodedhash.length);
        for (int i = 0; i < encodedhash.length; i++) {
            String hex = Integer.toHexString(0xff & encodedhash[i]);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString().toUpperCase(Locale.ROOT);
    }
}
