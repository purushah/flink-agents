/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.agents.runtime.actionstate;

import org.apache.flink.agents.api.InputEvent;
import org.apache.flink.agents.plan.actions.Action;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Cross-JVM regression test for the original failure (review): durable-state keys must be identical
 * when the same action is keyed from two DIFFERENT JVM processes, which is what recovery after a
 * process restart looks like. The in-JVM tests in {@link ActionStateUtilTest} pin the derivation;
 * they cannot catch a hash that folds in {@code Class.hashCode()}, because identity hashes are
 * stable within one JVM — the pre-fix bug only manifests across processes.
 */
class CrossJvmKeyStabilityTest {

    /** Entry point run in the child JVMs: prints the state key for a fixed action and event. */
    public static final class PrintKey {
        public static void main(String[] args) throws Exception {
            Action action = new NoOpAction("cross-jvm-action");
            InputEvent event = new InputEvent("cross-jvm-input");
            // Fixed key/seqNum/maxParallelism so the only possible variation is the action UUID.
            System.out.println(
                    "STATE_KEY="
                            + ActionStateUtil.generateKey("cross-jvm-key", 5, action, event, 8));
        }
    }

    @Test
    void sameActionYieldsSameKeyAcrossSeparateJvms() throws Exception {
        String first = keyFromFreshJvm();
        String second = keyFromFreshJvm();
        assertEquals(
                first,
                second,
                "Durable-state keys must be stable across JVM restarts, or recovery can never"
                        + " replay saved action results.");
    }

    private static String keyFromFreshJvm() throws Exception {
        String javaBin =
                Path.of(System.getProperty("java.home"), "bin", "java").toAbsolutePath().toString();
        List<String> command = new ArrayList<>();
        command.add(javaBin);
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(PrintKey.class.getName());
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        String key = null;
        StringBuilder output = new StringBuilder();
        try (BufferedReader reader =
                new BufferedReader(
                        new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append('\n');
                // Prefix-marked so child-JVM logging can never be mistaken for the key.
                if (line.startsWith("STATE_KEY=")) {
                    key = line.substring("STATE_KEY=".length()).trim();
                }
            }
        }
        int exit = process.waitFor();
        assertEquals(0, exit, "child JVM failed:\n" + output);
        assertNotNull(key, "child JVM printed no state key:\n" + output);
        return key;
    }
}
