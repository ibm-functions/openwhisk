/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.utils

import java.nio.charset.StandardCharsets
import java.security.SecureRandom
import java.util.Base64

import javax.crypto.Cipher
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec

object CryptHelpers {

  private val ALGORITHM_AES = "AES"
  private val ALGORITHM_AES_GCM_NOPADDING = "AES/GCM/NoPadding"
  private val ALGORITHM_NONCE_SIZE_12 = 12;
  private val ALGORITHM_TAG_SIZE_128 = 128;

  /**
   * AES encrypts string and returns it base64 encoded
   *
   * @param plaintext plain string
   * @param key key encryption key
   * @param iv intialization vector to be used instead of generating new random bytes
   *
   * @return Base64 encoded AES encrypted string
   *
   * @throws IllegalArgumentException
   * @throws InvalidAlgorithmParameterException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws NoSuchPaddingException
   *
   */
  def encryptString(plaintext: String, key: String, iv: String = ""): String = {
    // generate a 96-bit cipher
    val ivb = iv.getBytes()
    val nonce = new Array[Byte](ALGORITHM_NONCE_SIZE_12)
    if (ivb.size < nonce.size) {
      new SecureRandom().nextBytes(nonce)
    } else {
      Array.copy(ivb, ivb.size - nonce.size, nonce, 0, nonce.size)
    }
    // create the cipher instance and initialize
    val cipher = Cipher.getInstance(ALGORITHM_AES_GCM_NOPADDING)
    cipher.init(
      Cipher.ENCRYPT_MODE,
      new SecretKeySpec(key.getBytes(), ALGORITHM_AES),
      new GCMParameterSpec(ALGORITHM_TAG_SIZE_128, nonce))
    // encrypt and prepend nonce
    val ciphertext = cipher.doFinal(plaintext.getBytes())
    val nonceAndCiphertext = new Array[Byte](nonce.size + ciphertext.size)
    Array.copy(nonce, 0, nonceAndCiphertext, 0, nonce.size)
    Array.copy(ciphertext, 0, nonceAndCiphertext, nonce.size, ciphertext.size)
    // return base64 encoded AES encrypted string
    return Base64.getEncoder().encodeToString(nonceAndCiphertext)
  }

  /**
   * Decrypts base64 encoded AES encrypted string
   *
   * @param base64NonceAndCiphertext Base64 encoded AES encrypted string containing nonce and ciphertext
   * @param key key encryption key
   *
   * @return decrypted/plain string
   *
   * @throws IllegalArgumentException
   * @throws InvalidAlgorithmParameterException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws NoSuchPaddingException
   *
   */
  def decryptString(base64NonceAndCiphertext: String, key: String): String = {
    // decode
    val nonceAndCiphertext = Base64.getDecoder().decode(base64NonceAndCiphertext)
    // retrieve nonce and ciphertext
    val nonce = new Array[Byte](ALGORITHM_NONCE_SIZE_12)
    val ciphertext = new Array[Byte](nonceAndCiphertext.size - ALGORITHM_NONCE_SIZE_12)
    Array.copy(nonceAndCiphertext, 0, nonce, 0, nonce.size)
    Array.copy(nonceAndCiphertext, nonce.size, ciphertext, 0, ciphertext.size)
    // create cipher instance and initialize
    val cipher = Cipher.getInstance(ALGORITHM_AES_GCM_NOPADDING)
    cipher.init(
      Cipher.DECRYPT_MODE,
      new SecretKeySpec(key.getBytes(), ALGORITHM_AES),
      new GCMParameterSpec(ALGORITHM_TAG_SIZE_128, nonce))
    // decrypt and return
    return new String(cipher.doFinal(ciphertext), StandardCharsets.UTF_8)
  }
}
