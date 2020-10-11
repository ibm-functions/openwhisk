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

import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

object CryptHelpers {

  /**
   * Decrypts base64 encoded AES encrypted string.
   *
   * @param base64NonceAndCiphertext Base64 encoded AES encrypted string containing nonce and ciphertext
   * @param key key encrytion key value
   *
   * @return decrypted string
   *
   * @throws IllegalArgumentException
   * @throws InvalidAlgorithmParameterException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws NoSuchPaddingException
   *
   */
  @throws(classOf[IllegalArgumentException])
  @throws(classOf[InvalidAlgorithmParameterException])
  @throws(classOf[InvalidKeyException])
  @throws(classOf[NoSuchAlgorithmException])
  @throws(classOf[NoSuchPaddingException])
  def decryptString(base64NonceAndCiphertext: String, key: String): String = {
    // decode
    val nonceAndCiphertext = Base64.getDecoder().decode(base64NonceAndCiphertext);
    // retrieve nonce and ciphertext
    val nonce = new Array[Byte](12)
    val ciphertext = new Array[Byte](nonceAndCiphertext.length - 12);
    Array.copy(nonceAndCiphertext, 0, nonce, 0, nonce.size)
    Array.copy(nonceAndCiphertext, nonce.size, ciphertext, 0, ciphertext.size)
    // create cipher instance and initialize
    val cipher = Cipher.getInstance("AES/GCM/NoPadding");
    cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key.getBytes(), "AES"), new GCMParameterSpec(128, nonce));
    // decrypt and return
    return new String(cipher.doFinal(ciphertext), StandardCharsets.UTF_8);
  }
}
