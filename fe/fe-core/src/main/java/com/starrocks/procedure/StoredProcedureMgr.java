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

package com.starrocks.procedure;

import com.starrocks.catalog.Database;
import com.starrocks.common.DdlException;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Creates and drops stored procedures. A procedure lives in the map of its {@link Database}, which is persisted with
 * the database in the image; changes go through the edit log ({@code OP_CREATE_PROCEDURE},
 * {@code OP_DROP_PROCEDURE}). The leader serializes changes with one lock, which is enough for a DDL this rare.
 */
public final class StoredProcedureMgr {
    private static final Logger LOG = LogManager.getLogger(StoredProcedureMgr.class);
    private static final Object LOCK = new Object();

    private StoredProcedureMgr() {
    }

    public static StoredProcedure get(Database db, String name) {
        return db.getStoredProcedures().get(StoredProcedure.key(name));
    }

    public static List<StoredProcedure> list(Database db) {
        List<StoredProcedure> procedures = new ArrayList<>(db.getStoredProcedures().values());
        procedures.sort(Comparator.comparing(StoredProcedure::getName, String.CASE_INSENSITIVE_ORDER));
        return procedures;
    }

    /**
     * Adds {@code procedure} to {@code db}. An existing procedure of that name is replaced when {@code orReplace},
     * kept when {@code ifNotExists}, and an error otherwise.
     */
    public static void create(Database db, StoredProcedure procedure, boolean orReplace, boolean ifNotExists)
            throws DdlException {
        synchronized (LOCK) {
            StoredProcedure existing = get(db, procedure.getName());
            if (existing != null && !orReplace) {
                if (ifNotExists) {
                    return;
                }
                throw new DdlException("Procedure " + existing.getName() + " already exists in database "
                        + db.getFullName());
            }
            GlobalStateMgr.getCurrentState().getEditLog().logCreateProcedure(procedure,
                    wal -> replayCreate((StoredProcedure) wal));
        }
        LOG.info("Created procedure {} in database {}", procedure.getSignature(), db.getFullName());
    }

    /** Drops the procedure {@code name}; returns false when there is none. */
    public static boolean drop(Database db, String name) {
        synchronized (LOCK) {
            StoredProcedure existing = get(db, name);
            if (existing == null) {
                return false;
            }
            GlobalStateMgr.getCurrentState().getEditLog().logDropProcedure(
                    new DropProcedureLog(db.getId(), existing.getName()), wal -> replayDrop((DropProcedureLog) wal));
            LOG.info("Dropped procedure {} from database {}", existing.getSignature(), db.getFullName());
            return true;
        }
    }

    public static void replayCreate(StoredProcedure procedure) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(procedure.getDbId());
        if (db == null) {
            LOG.warn("Skip replaying procedure {}: database {} does not exist", procedure.getName(),
                    procedure.getDbId());
            return;
        }
        db.getStoredProcedures().put(StoredProcedure.key(procedure.getName()), procedure);
    }

    public static void replayDrop(DropProcedureLog log) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(log.getDbId());
        if (db != null) {
            db.getStoredProcedures().remove(StoredProcedure.key(log.getName()));
        }
    }
}
