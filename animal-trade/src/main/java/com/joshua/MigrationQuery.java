package com.joshua;

import java.util.Map;

/*
basic goal:

void (?) insert1(Map<String, String> line) {
    insert = "insert $x isa Country, has name \"" + line['id'] + "\",";
 */

public abstract class MigrationQuery {

    // instantiate with a user function
    // which takes a CSVLine which is indexable by the column names
    // idea is that they write a query either as a string
    // which can

    abstract String getQuery(Map<String, String> line);

}




