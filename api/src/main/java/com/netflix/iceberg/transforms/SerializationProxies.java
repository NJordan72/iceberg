/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.transforms;

import java.io.ObjectStreamException;
import java.io.Serializable;

/**
 * Stand-in classes for expression classes in Java Serialization.
 * <p>
 * These are used so that transform classes can be singletons and use identical equality.
 */
class SerializationProxies {
  static class IdentityProxy implements Serializable {
    private static final IdentityProxy INSTANCE = new IdentityProxy();

    static IdentityProxy get() {
      return INSTANCE;
    }

    /**
     * Constructor for Java serialization.
     */
    public IdentityProxy() {
    }

    Object readResolve() throws ObjectStreamException {
      return Identity.get();
    }
  }
}