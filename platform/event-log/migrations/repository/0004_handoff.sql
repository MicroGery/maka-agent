-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.


ALTER TABLE checkpoint_repository_heads RENAME TO checkpoint_repository_heads_previous;
CREATE TABLE checkpoint_repository_heads (
    session_id TEXT PRIMARY KEY NOT NULL REFERENCES checkpoint_repository_sessions(session_id),
    head_json TEXT NOT NULL CHECK(json_valid(head_json)),
    source_key TEXT NOT NULL REFERENCES checkpoint_repository_sources(source_key)
);
INSERT INTO checkpoint_repository_heads(session_id,head_json,source_key)
SELECT h.session_id,h.head_json,s.source_key
FROM checkpoint_repository_heads_previous h
JOIN checkpoint_repository_sources s USING(session_id);
DROP TABLE checkpoint_repository_heads_previous;
ALTER TABLE checkpoint_execution_claims ADD COLUMN handoff_json TEXT CHECK(handoff_json IS NULL OR json_valid(handoff_json));
