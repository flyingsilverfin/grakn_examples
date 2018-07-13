package com.joshua;

import java.util.Map;

public abstract class SingletonInsertMigrationQuery extends MigrationQuery {

    abstract String getCheckExistQuery(Map<String, String> line);
}
