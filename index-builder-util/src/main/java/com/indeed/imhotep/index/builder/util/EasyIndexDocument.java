/*
 * Copyright (C) 2014 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package com.indeed.imhotep.index.builder.util;

import java.util.HashMap;

/**
 * @author mmorrison
 */
public class EasyIndexDocument extends HashMap<String,String> {
    public long timestamp = 0;
    public String uid;

    public void put(String key, long value) {
        put(key, Long.toString(value, 10));
    }
    public void put(String key, int value) {
        put(key, Integer.toString(value, 10));
    }
    public void put(String key, CharSequence value) {
        put(key, value.toString());
    }
}
