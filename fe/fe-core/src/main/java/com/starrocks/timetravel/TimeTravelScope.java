// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.timetravel;

import com.starrocks.qe.ConnectContext;

/** The planning of one statement: time travel versions pinned inside it do not outlive it. */
public final class TimeTravelScope implements AutoCloseable {
    private final ConnectContext session;

    private TimeTravelScope(ConnectContext session) {
        this.session = session;
        session.enterTimeTravelScope();
    }

    public static TimeTravelScope open(ConnectContext session) {
        return new TimeTravelScope(session);
    }

    @Override
    public void close() {
        session.exitTimeTravelScope();
    }
}
