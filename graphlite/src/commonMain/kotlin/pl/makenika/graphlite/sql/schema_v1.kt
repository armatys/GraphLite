/*
 * Copyright (c) 2020 Mateusz Armatys
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pl.makenika.graphlite.sql

//language=SQLITE-SQL
internal val schemaV1 = arrayOf(
    """create table if not exists Schema (
  id varchar primary key not null,
  handle varchar not null,
  version int not null
);""",

    "create unique index if not exists schemaIndex on Schema (handle, version);",

    """create table if not exists Field (
  id varchar primary key not null,
  handle varchar not null,
  schemaId varchar not null,
  type varchar(16) not null, -- blob, integer, real, text, geo
  foreign key (schemaId) references Schema (id) on delete cascade
);""",

    "create unique index if not exists fieldIndex on Field (schemaId, handle);",

    """create table if not exists Element (
  id varchar primary key not null,
  handle varchar not null unique,
  schemaId varchar not null,
  type varchar not null,
  foreign key (schemaId) references Schema (id) on delete cascade
);""",

    "create index if not exists elementSchemaNameIndex on Element (schemaId, handle);",
    "create index if not exists elementSchemaTypeIndex on Element (schemaId, type);",

    """create table if not exists Connection (
  id varchar primary key not null,
  edgeHandle varchar not null,
  nodeHandle varchar not null,
  outgoing boolean,
  foreign key (edgeHandle) references Element (handle) on delete cascade,
  foreign key (nodeHandle) references Element (handle) on delete cascade
);""",

    "create unique index if not exists edgeNodeIndex on Connection (edgeHandle, nodeHandle);",
    "create unique index if not exists nodeEdgeIndex on Connection (nodeHandle, edgeHandle);",
    "create index if not exists edgeOutgoingIndex on Connection (edgeHandle, outgoing);",
    "create index if not exists nodeOutgoingIndex on Connection (nodeHandle, outgoing);"
)

// TODO log table, or use Session extension https://www.sqlite.org/sessionintro.html
// operation (create, update, delete)
// table name
// id
// timestamp

internal fun getFieldValueTableName(fieldId: String): String = "Field_${safeTableName(fieldId)}"
internal fun getFtsTableName(fieldId: String): String = "FieldFtsText_${safeTableName(fieldId)}"
internal fun getRTreeTableName(fieldId: String): String = "FieldGeo_${safeTableName(fieldId)}_rtree"

internal fun createTableFieldValueBlob(fieldId: String, isValueOptional: Boolean): Array<String> {
    val notNull = if (isValueOptional) "" else "not null"
    val tableName = getFieldValueTableName(fieldId)
    //language=SQLITE-SQL
    return arrayOf(
        """
        create table if not exists $tableName (
          id varchar primary key not null,
          elementHandle varchar not null unique,
          value blob $notNull,
          foreign key (elementHandle) references Element (handle) on delete cascade
        );"""
    )
}

internal fun createTableFieldValueLongInt(fieldId: String, isValueOptional: Boolean): Array<String> {
    val notNull = if (isValueOptional) "" else "not null"
    val safeFieldId = safeTableName(fieldId)
    val tableName = getFieldValueTableName(fieldId)
    //language=SQLITE-SQL
    return arrayOf(
        """
        create table if not exists $tableName (
          id varchar primary key not null,
          elementHandle varchar not null unique,
          value integer $notNull,
          foreign key (elementHandle) references Element (handle) on delete cascade
        );""",

        "create index if not exists fieldValueIntegerIndex_$safeFieldId on $tableName (value)"
    )
}

internal fun createTableFieldValueDoubleFloat(fieldId: String, isValueOptional: Boolean): Array<String> {
    val notNull = if (isValueOptional) "" else "not null"
    val safeFieldId = safeTableName(fieldId)
    val tableName = getFieldValueTableName(fieldId)
    //language=SQLITE-SQL
    return arrayOf(
        """
        create table if not exists $tableName (
          id varchar primary key not null,
          elementHandle varchar not null unique,
          value real $notNull,
          foreign key (elementHandle) references Element (handle) on delete cascade
        );""",

        "create index if not exists fieldValueRealIndex_$safeFieldId on $tableName (value)"
    )
}

internal fun createTableFieldValueText(
    fieldId: String,
    isValueOptional: Boolean,
    fts: Boolean
): Array<String> {
    val notNull = if (isValueOptional) "" else "not null"
    val safeFieldId = safeTableName(fieldId)
    val tableName = getFieldValueTableName(fieldId)
    val ftsTableName = getFtsTableName(fieldId)

    val ftsStatements by lazy {
        //language=SQLITE-SQL
        arrayOf(
            """create virtual table if not exists $ftsTableName using fts5 (
          value,
          content=$tableName
        );""",

            """CREATE TRIGGER ${tableName}_bu BEFORE UPDATE ON $tableName BEGIN
            DELETE FROM $ftsTableName WHERE rowid=old.rowid;
        END;""",

            """CREATE TRIGGER ${tableName}_bd BEFORE DELETE ON $tableName BEGIN
            DELETE FROM $ftsTableName WHERE rowid=old.rowid;
        END;""",

            """CREATE TRIGGER ${tableName}_au AFTER UPDATE ON $tableName BEGIN
            INSERT INTO $ftsTableName(rowid, value) VALUES(new.rowid, new.value);
        END;""",

            """CREATE TRIGGER ${tableName}_ai AFTER INSERT ON $tableName BEGIN
            INSERT INTO $ftsTableName(rowid, value) VALUES(new.rowid, new.value);
        END;"""
        )
    }

    //language=SQLITE-SQL
    return arrayOf(
        """
        create table if not exists $tableName (
          id varchar primary key not null,
          elementHandle varchar not null unique,
          value text $notNull,
          foreign key (elementHandle) references Element (handle) on delete cascade
        );""",

        "create index if not exists fieldValueTextIndex_$safeFieldId on $tableName (value)"
    ) + if (fts) ftsStatements else emptyArray()
}

internal fun createTableFieldValueGeo(fieldId: String, isValueOptional: Boolean): Array<String> {
    val notNull = if (isValueOptional) "" else "not null"
    val safeFieldId = safeTableName(fieldId)
    val tableName = getFieldValueTableName(fieldId)
    val rtreeTableName = getRTreeTableName(fieldId)
    //language=SQLITE-SQL
    return arrayOf(
        """
        create table if not exists $tableName (
          id varchar primary key not null,
          elementHandle varchar not null unique,
          minLat real $notNull,
          maxLat real $notNull,
          minLon real $notNull,
          maxLon real $notNull,
          foreign key (elementHandle) references Element (handle) on delete cascade
        );""",

        "create index if not exists fieldValueGeoIndex_$safeFieldId on $tableName (minLat, maxLat, minLon, maxLon)",

        """create virtual table if not exists $rtreeTableName using rtree(
            id,
            minLat,
            maxLat,
            minLon,
            maxLon
        );""",

        """create trigger FieldGeo_${safeFieldId}_ai after insert on $tableName begin
          insert into FieldGeo_${safeFieldId}_rtree(rowid, minLat, maxLat, minLon, maxLon) values (new.rowid, new.minLat, new.maxLat, new.minLon, new.maxLon);
        end;""",

        """create trigger FieldGeo_${safeFieldId}_ad after delete on $tableName begin
          DELETE FROM FieldGeo_${safeFieldId}_rtree WHERE rowid=old.rowid;
        end;""",

        """create trigger FieldGeo_${safeFieldId}_au after update on $tableName begin
          DELETE FROM FieldGeo_${safeFieldId}_rtree WHERE rowid=old.rowid;
          insert into FieldGeo_${safeFieldId}_rtree(rowid, minLat, maxLat, minLon, maxLon) values (new.rowid, new.minLat, new.maxLat, new.minLon, new.maxLon);
        end;"""
    )
}

private fun safeTableName(fieldId: String): String = fieldId.replace(Regex("[^a-zA-Z0-9]"), "")
