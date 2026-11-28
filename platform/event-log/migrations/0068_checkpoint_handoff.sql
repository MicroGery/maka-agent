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


CREATE TABLE checkpoint_handoffs (
    operation TEXT PRIMARY KEY NOT NULL,
    request_json TEXT NOT NULL CHECK(json_valid(request_json)),
    prepared_json TEXT NOT NULL CHECK(json_valid(prepared_json)),
    directory_identity_json TEXT CHECK(directory_identity_json IS NULL OR json_valid(directory_identity_json)),
    activated_operation TEXT,
    CHECK(activated_operation IS NULL OR directory_identity_json IS NOT NULL)
);
CREATE UNIQUE INDEX checkpoint_handoffs_activated_generation
    ON checkpoint_handoffs(json_extract(request_json,'$.grant.activation'))
    WHERE activated_operation IS NOT NULL;
