package grakn.examples.animaltrade;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DataMigrationQueries {

    // Note: these must be in increasing specialization of the single-instance hierarchies!
    static MigrationQuery[] getCountryRegionMigrationQueries() {
        return new MigrationQuery[] {
                new ContinentInsertQuery(), // region first, the higher level in the hierarchy
                new CountryInsertQuery() // then the country
        };
    }

    static MigrationQuery[] getTaxonomyHierarchyMigrationQueries() {
        return new MigrationQuery[] {
                new ClassInsertQuery(),
                new OrderInsertQuery(),
                new FamilyInsertQuery(),
                new GenusInsertQuery(),
                new SpeciesInsertQuery()
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
        // join date and participates-CITES and CITES-partipation-type

        String participationType = line.get("Type");
        if (!participationType.trim().isEmpty()) {
            bs.add(", has CITES-participation-type ");
            bs.addQuoted(participationType);
            bs.add(", has CITES-participation true");
            bs.add(", has CITES-entry-into-force-date  ");
            // need to parse out date
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
            LocalDate date = LocalDate.from(formatter.parse(line.get("Entry into force")));
            DateTimeFormatter graqlDateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            String graqlDateString = graqlDateFormat.format(date).toString();
            bs.add(graqlDateString);
        } else {
            bs.add(", has CITES-participation false");
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
        bs.add("match $cont isa continent, has name ");
        bs.addQuoted(line.get("Region"));
        bs.add("; insert ");
        bs.add(this.sharedString(line));
        bs.add("(containing-continent: $cont, contained-country: $c) isa continent-country-containment;");
        return bs.toString();
    }
}

// intended for use with auxiliary CSV with Country - Region mapping
class ContinentInsertQuery extends SingletonInsertMigrationQuery {
    private String sharedString(Map<String, String> line) {
        BuildString bs = new BuildString();
        bs.add("$r isa continent, has name ");
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
        bs.add("$c isa taxonomic-class, has name ");
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
        bs.add("$x isa taxonomic-order, has name ");
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
        bs.add("match $c isa taxonomic-class, has name ");
        bs.addQuoted(line.get("Class"));
        bs.add("; insert ");
        bs.add(this.sharedString(line));
        bs.add(" (super-taxon: $c, sub-taxon: $x) isa taxonomic-hierarchy;");
        return bs.toString();
    }
}

class FamilyInsertQuery extends SingletonInsertMigrationQuery {
    private String sharedString(Map<String, String> line) {
        BuildString bs = new BuildString();
        bs.add("$f isa taxonomic-family, has name ");
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
        bs.add("match $x isa taxonomic-order, has name ");
        bs.addQuoted(line.get("Order"));
        bs.add("; insert ");
        bs.add(this.sharedString(line));
        bs.add(" (super-taxon: $x, sub-taxon: $f) isa taxonomic-hierarchy;");
        return bs.toString();
    }
}

class GenusInsertQuery extends SingletonInsertMigrationQuery {
    private String sharedString(Map<String, String> line) {
        BuildString bs = new BuildString();
        bs.add("$g isa taxonomic-genus, has name ");
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
        bs.add("match $f isa taxonomic-family, has name ");
        bs.addQuoted(line.get("Family"));
        bs.add("; insert ");
        bs.add(this.sharedString(line));
        bs.add(" (super-taxon: $f, sub-taxon: $g) isa taxonomic-hierarchy;");
        return bs.toString();
    }
}

class SpeciesInsertQuery extends SingletonInsertMigrationQuery {
    private String sharedString(Map<String, String> line) {
        BuildString bs = new BuildString();
        bs.add("$t isa taxonomic-species, has name ");
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
        bs.add("match $g isa taxonomic-genus, has name ");
        bs.addQuoted(line.get("Genus"));
        bs.add("; insert ");
        bs.add(this.sharedString(line));
        bs.add(" (super-taxon: $g, sub-taxon: $t) isa taxonomic-hierarchy;");
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
        bs.add("$s isa taxonomic-species, has name ");
        bs.addQuoted(line.get("Taxon"));
        bs.add("; ");

        // insert
        bs.add("insert ");
        bs.add("$m isa measurement, has unit-of-measurement ");
        bs.addQuoted(line.get("Unit"));
        bs.add(", has measured-quantity " + Double.parseDouble(line.get("Importer reported quantity")));
        bs.add("; ");
        bs.add("$item isa traded-item, has item-purpose ");
        bs.addQuoted(line.get("Purpose"));
        bs.add(", has item-source ");
        bs.addQuoted(line.get("Source"));
        bs.add(", has item-type");
        bs.addQuoted(line.get("Term"));
        bs.add("; ");
        bs.add("$import(imported-item: $item, receiving-country: $importer, providing-country: $exporter) isa import, has exchange-date ");
        int year = Integer.parseInt(line.get("Year"));
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate date = LocalDate.of(year, 1, 1);
        String dateString = date.format(formatter);
        bs.add(dateString);
        bs.add(", has appendix ");
        bs.add(appendixMapping().get(line.get("App.")));
        bs.add("; ");
        bs.add("$r (quantified-subject: $item, quantification-measurement: $m) isa quantification; ") ;
        bs.add("(member-item: $item, taxonomic-group: $s) isa taxon-membership;");
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
        bs.add("$s isa taxonomic-species, has name ");
        bs.addQuoted(line.get("Taxon"));
        bs.add("; ");

        // insert
        bs.add("insert ");
        bs.add("$m isa measurement, has unit-of-measurement ");
        bs.addQuoted(line.get("Unit"));
        bs.add(", has measured-quantity " + Double.parseDouble(line.get("Exporter reported quantity"))); // ** difference **
        bs.add("; ");
        bs.add("$item isa traded-item, has item-purpose ");
        bs.addQuoted(line.get("Purpose"));
        bs.add(", has item-source ");
        bs.addQuoted(line.get("Source"));
        bs.add(", has item-type ");
        bs.addQuoted(line.get("Term"));
        bs.add("; ");
        bs.add("$export (exported-item: $item, receiving-country: $importer, providing-country: $exporter) isa export, has exchange-date "); // ** difference **
        int year = Integer.parseInt(line.get("Year"));
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate date = LocalDate.of(year, 1, 1);
        String dateString = date.format(formatter);
        bs.add(dateString);
        bs.add(", has appendix ");
        bs.add(appendixMapping().get(line.get("App.")));
        bs.add("; ");
        bs.add("$r (quantified-subject: $item, quantification-measurement: $m) isa quantification; ");
        bs.add("(member-item: $item, taxonomic-group: $s) isa taxon-membership;");
        return bs.toString();
    }
}



