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

/**
 * @author mmorrison
 *
 * This is a stub interface for classes which can
 * be bootstrapped by the Bootstrap class
 *
 * Read {@link Bootstrap} for more info
 */
public interface Bootable {
    // Implementing classes should either define
    // public void run(String[] args)
    // or
    // public int run(String[] args)

    // the implemented run function can optionally throw Exceptions,
    // just like main() normally would

    // if run() returns an integer, the value will be returned
    // as the application's exit code
}
