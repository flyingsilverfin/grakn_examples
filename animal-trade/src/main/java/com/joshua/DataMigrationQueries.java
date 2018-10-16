package com.joshua;



import com.google.common.collect.ImmutableMap;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DataMigrationQueries {

    // Note: these must be in increasing specialization of the single-instance hierarchies!
    static MigrationQuery[] getCountryRegionMigrationQueries() {
        return new MigrationQuery[] {
                new RegionInsertQuery(), // region first, the higher level in the hierarchy
                new CountryInsertQuery() // then the country
        };
    }

    static MigrationQuery[] getTaxonomyHierarchyMigrationQueries() {
        return new MigrationQuery[] {
                new ClassInsertQuery(),
                new OrderInsertQuery(),
                new FamilyInsertQuery(),
                new GenusInsertQuery(),
                new TaxonInsertQuery()
        };
    }

    static MigrationQuery getImportMigrationQuery() {
        return new MainImportQuery();
    }

    static MigrationQuery getExportMigrationQuery() {
        return new MainExportQuery();
    }
}

/*
 * Helper to build query strings
 * provides easy quote-wrapped strings in strings
 */
class BuildString {
    StringBuilder sb;
    public BuildString() {
        sb = new StringBuilder();
    }
    public void add(Object s) {
        sb.append(s);
    }
    public void addQuoted(String s) {
        sb.append("\"");
        sb.append(s);
        sb.append("\"");
    }
    @Override
    public String toString() {
        return sb.toString();
    }
}


class AddRelationshipQuery {
    public static String getAddRelationshipQuery(String relationship, List<String> roles, List<String> entityConceptIds) {

        BuildString bs = new BuildString();
        bs.add("match ");
        String var = "$x";
        for (String conceptId : entityConceptIds) {
            bs.add(var);
            bs.add(conceptId); // compounded variable name to be $xVS121323...

            bs.add(" id ");
            bs.add(conceptId);
            bs.add("; ");
        }
        bs.add("insert (");
        Iterator<String> roleIter = roles.iterator();
        Iterator<String> conceptIdsIter = entityConceptIds.iterator();
        while (roleIter.hasNext() && conceptIdsIter.hasNext()) {
            String role = roleIter.next();
            String conceptId = conceptIdsIter.next();
            bs.add(role);
            bs.add(": ");
            bs.add(var);
            bs.add(conceptId); // compound variable name as above
            if (roleIter.hasNext()) {
                bs.add(", ");
            }
        }
        bs.add(") isa ");
        bs.add(relationship);
        bs.add(";");

        return bs.toString();
    }
}


/*
 *
 * Insert singleton entities -- here, each of these have a "check" to do to see if they exist
 * If they do, don't need to insert again!
 * Otherwise, can use the standard query that does the insertion/migration
 */


// intended for use with auxiliary CSV with Country - Region mapping
class CountryInsertQuery extends SingletonInsertMigrationQuery {
    private String sharedString(Map<String, String> line) {
        BuildString bs = new BuildString();
        bs.add("$c isa country, has name ");
        bs.addQuoted(line.get("Official Name"));
        bs.add(", has ISO-id ");
        bs.addQuoted(line.get("ISO"));
        // join date and participates-CITIES and CITIES-partipation-type

        String participationType = line.get("Type");
        if (!participationType.trim().isEmpty()) {
            bs.add(", has CITIES-participation-type ");
            bs.addQuoted(participationType);
            bs.add(", has participates-CITIES true");
            bs.add(", has CITIES-entry-into-force-date  ");
            // need to parse out date
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
            LocalDate date = LocalDate.from(formatter.parse(line.get("Entry into force")));
            DateTimeFormatter graqlDateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            String graqlDateString = graqlDateFormat.format(date).toString();
            bs.add(graqlDateString);
        }
        bs.add(";");
        return bs.toString();
    }
    @Override
    public String getCheckExistQuery(Map<String, String> line) {
        return "match " + this.sharedString(line) + " get;";
    }
    @Override
    public String getQuery(Map<String, String> line) {
        // assume this query is only run ONCE per unique country name
        // and that the region already exists!
        // insert the hierarchy relationship
        // TODO this is probably not good practice, should put this behavior somewhere else
        // TODO however is convenient since have access to $c for country
        // TODO but again this is brittle since $c is generated in a different method `sharedString`...
        BuildString bs = new BuildString();
        bs.add("match $r isa region, has name ");
        bs.addQuoted(line.get("Region"));
        bs.add("; insert ");
        bs.add(this.sharedString(line));
        bs.add("(container: $r, containee: $c) isa location-hierarchy;");
        return bs.toString();
    }
}

// intended for use with auxiliary CSV with Country - Region mapping
class RegionInsertQuery extends SingletonInsertMigrationQuery {
    private String sharedString(Map<String, String> line) {
        BuildString bs = new BuildString();
        bs.add("$r isa region, has name ");
        bs.addQuoted(line.get("Region")); // TODO check if this is the right key
        bs.add(";");
        return bs.toString();
    }

    @Override
    public String getCheckExistQuery(Map<String, String> line) {
        return "match " + this.sharedString(line) + " get;";
    }
    @Override
    public String getQuery(Map<String, String> line) {
        return "insert " + this.sharedString(line); //hacks
    }
}


// For main CSV with animal import/export data
class ClassInsertQuery extends SingletonInsertMigrationQuery {
    private String sharedString(Map<String, String> line) {
        BuildString bs = new BuildString();
        bs.add("$c isa taxonomy-class, has name ");
        bs.addQuoted(line.get("Class")); // TODO check if this is the right key
        bs.add(";");
        return bs.toString();
    }

    @Override
    public String getCheckExistQuery(Map<String, String> line) {
        return "match " + this.sharedString(line) + " get;";
    }
    @Override
    public String getQuery(Map<String, String> line) {
        return  "insert " + this.sharedString(line);
    }
}

class OrderInsertQuery extends SingletonInsertMigrationQuery {
    private String sharedString(Map<String, String> line) {
        BuildString bs = new BuildString();
        bs.add("$x isa taxonomy-order, has name ");
        bs.addQuoted(line.get("Order")); // TODO check if this is the right key
        bs.add(";");
        return bs.toString();
    }

    @Override
    public String getCheckExistQuery(Map<String, String> line) {
        return "match " + this.sharedString(line) + " get;";
    }
    @Override
    public String getQuery(Map<String, String> line) {
        // get the query to insert $x, the order
        // assume this is only called once, and that super (`class`) is already inserted once
        BuildString bs = new BuildString();
        bs.add("match $c isa taxonomy-class, has name ");
        bs.addQuoted(line.get("Class"));
        bs.add("; insert ");
        bs.add(this.sharedString(line));
        bs.add(" (ancestor: $c, descendant: $x) isa taxonomy-hierarchy;");
        return bs.toString();
    }
}

class FamilyInsertQuery extends SingletonInsertMigrationQuery {
    private String sharedString(Map<String, String> line) {
        BuildString bs = new BuildString();
        bs.add("$f isa taxonomy-family, has name ");
        bs.addQuoted(line.get("Family")); // TODO check if this is the right key
        bs.add(";");
        return bs.toString();
    }

    @Override
    public String getCheckExistQuery(Map<String, String> line) {
        return "match " + this.sharedString(line) + " get;";
    }
    @Override
    public String getQuery(Map<String, String> line) {
        // get the query to insert $x, the order
        // assume this is only called once, and that super (`class`) is already inserted once
        BuildString bs = new BuildString();
        bs.add("match $x isa taxonomy-order, has name ");
        bs.addQuoted(line.get("Order"));
        bs.add("; insert ");
        bs.add(this.sharedString(line));
        bs.add(" (ancestor: $x, descendant: $f) isa taxonomy-hierarchy;");
        return bs.toString();
    }
}

class GenusInsertQuery extends SingletonInsertMigrationQuery {
    private String sharedString(Map<String, String> line) {
        BuildString bs = new BuildString();
        bs.add("$g isa taxonomy-genus, has name ");
        bs.addQuoted(line.get("Genus")); // TODO check if this is the right key
        bs.add(";");
        return bs.toString();
    }

    @Override
    public String getCheckExistQuery(Map<String, String> line) {
        return "match " + this.sharedString(line) + " get;";
    }
    @Override
    public String getQuery(Map<String, String> line) {
        // assume this is only called once, and that super (`class`) is already inserted once
        BuildString bs = new BuildString();
        bs.add("match $f isa taxonomy-family, has name ");
        bs.addQuoted(line.get("Family"));
        bs.add("; insert ");
        bs.add(this.sharedString(line));
        bs.add(" (ancestor: $f, descendant: $g) isa taxonomy-hierarchy;");
        return bs.toString();
    }
}

class TaxonInsertQuery extends SingletonInsertMigrationQuery {
    private String sharedString(Map<String, String> line) {
        BuildString bs = new BuildString();
        bs.add("$t isa taxonomy-taxon, has name ");
        bs.addQuoted(line.get("Taxon")); // TODO check if this is the right key
        bs.add(";");
        return bs.toString();
    }

    @Override
    public String getCheckExistQuery(Map<String, String> line) {
        return "match " + this.sharedString(line) + " get;";
    }
    @Override
    public String getQuery(Map<String, String> line) {
        // assume this is only called once, and that super (`class`) is already inserted once
        BuildString bs = new BuildString();
        bs.add("match $g isa taxonomy-genus, has name ");
        bs.addQuoted(line.get("Genus"));
        bs.add("; insert ");
        bs.add(this.sharedString(line));
        bs.add(" (ancestor: $g, descendant: $t) isa taxonomy-hierarchy;");
        return bs.toString();
    }
}

/*
 * Insertions that are NOT singletons (ie upserts)
 */



// insert majority of information


class MainImportQuery extends MigrationQuery {
    @Override
    public String getQuery(Map<String, String> line) {
        BuildString bs = new BuildString();
        // match
        bs.add("match $importer isa country, has ISO-id ");
        bs.addQuoted(line.get("Importer"));
        bs.add("; ");
        bs.add("$exporter isa country, has ISO-id ");
        bs.addQuoted(line.get("Exporter"));
        bs.add("; ");
        bs.add("$s isa taxonomy-taxon, has name ");
        bs.addQuoted(line.get("Taxon"));
        bs.add("; ");

        // insert
        bs.add("insert ");
        bs.add("$m isa measure, has unit ");
        bs.addQuoted(line.get("Unit"));
        bs.add(", has quantity " + line.get("Importer reported quantity"));
        bs.add("; ");
        bs.add("$item isa animal-item, has purpose ");
        bs.addQuoted(line.get("Purpose"));
        bs.add(", has source ");
        bs.addQuoted(line.get("Source"));
        bs.add(", has term ");
        bs.addQuoted(line.get("Term"));
        bs.add("; ");
        bs.add("$import(object: $item, receiver: $importer, provider: $exporter) isa import, has exch-date ");
        int year = Integer.parseInt(line.get("Year"));
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate date = LocalDate.of(year, 1, 1);
        String dateString = date.format(formatter);
        bs.add(dateString);
        bs.add(", has appendix ");
        bs.add(appendixMapping.get(line.get("App.")));
        bs.add("; ");
        bs.add("$r (sized: $item, measurement: $m) isa sizing; ") ;
        bs.add("(target: $item, explanation: $s) isa description;");
        return bs.toString();
    }
}

class MainExportQuery extends MigrationQuery {
    @Override
    public String getQuery(Map<String, String> line) {
        BuildString bs = new BuildString();
        // match
        bs.add("match $importer isa country, has ISO-id ");
        bs.addQuoted(line.get("Importer"));
        bs.add("; ");
        bs.add("$exporter isa country, has ISO-id ");
        bs.addQuoted(line.get("Exporter"));
        bs.add("; ");
        bs.add("$s isa taxonomy-taxon, has name ");
        bs.addQuoted(line.get("Taxon"));
        bs.add("; ");

        // insert
        bs.add("insert ");
        bs.add("$m isa measure, has unit ");
        bs.addQuoted(line.get("Unit"));
        bs.add(", has quantity " + line.get("Exporter reported quantity")); // ** difference **
        bs.add("; ");
        bs.add("$item isa animal-item, has purpose ");
        bs.addQuoted(line.get("Purpose"));
        bs.add(", has source ");
        bs.addQuoted(line.get("Source"));
        bs.add(", has term ");
        bs.addQuoted(line.get("Term"));
        bs.add("; ");
        bs.add("$export (object: $item, receiver: $importer, provider: $exporter) isa export has exch-date "); // ** difference **
        int year = Integer.parseInt(line.get("Year"));
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate date = LocalDate.of(year, 1, 1);
        String dateString = date.format(formatter);
        bs.add(dateString);
        bs.add(", has appendix ");
        bs.add(appendixMapping.get(line.get("App.")));
        bs.add("; ");
        bs.add("$r (sized: $item, measurement: $m) isa sizing; ");
        bs.add("(target: $item, explanation: $s) isa description;");
        return bs.toString();
    }
}



