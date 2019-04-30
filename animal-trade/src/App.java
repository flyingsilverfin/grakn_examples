package grakn.examples.animaltrade;

import grakn.client.GraknClient;
import grakn.core.concept.Concept;
import grakn.core.concept.answer.ConceptMap;
import graql.lang.Graql;
import graql.lang.query.GraqlGet;
import graql.lang.query.GraqlInsert;
import graql.lang.query.GraqlQuery;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static graql.lang.Graql.parseList;


/**
 * TODO
 * * Add cmd flag for splitting into separate keyspaces & probabilities (up to 3 - train/test/validate)
 * * refactor for cleaner architecture
 * * Renaming
 */
public class App {
    public static void main(String[] args) throws IOException {
        final String GRAKN_URI = "localhost:48555";
        final String TRAIN_KEYSPACE = "animaltrade_train";
        final String TEST_KEYSPACE = "animaltrade_test";
        final double TRAIN_SPLIT = 0.5;

        GraknClient.Session trainKeyspace = new GraknClient(GRAKN_URI).session(TRAIN_KEYSPACE);
        GraknClient.Session testKeyspace = new GraknClient(GRAKN_URI).session(TEST_KEYSPACE);

        loadSchema(trainKeyspace);
        loadSchema(testKeyspace);

        loadCountryRegions(trainKeyspace); // make two load datas: one for each file to load data from
        loadCountryRegions(testKeyspace);
        loadAnimalTradeData(trainKeyspace, testKeyspace, TRAIN_SPLIT);
        trainKeyspace.close();
        testKeyspace.close();

    }

    private static void loadSchema(GraknClient.Session session) {

        Path schemaPath = Paths.get("./data/schema.gql");
        List<String> schemaQueries = null;
        try {
            System.out.println("Loading schema");
            schemaQueries = Files.readAllLines(schemaPath, StandardCharsets.UTF_8);
            try (GraknClient.Transaction tx = session.transaction().write()) {
                Stream<GraqlQuery> query = parseList(schemaQueries.stream().collect(Collectors.joining("\n")));
                query.forEach(q -> tx.execute(q));
                tx.commit();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void loadCountryRegions(GraknClient.Session session) throws IOException {
        String countryRegionCSV = "/Users/joshua/Documents/grakn_examples/animal-trade/data/country_region_mapping.csv";
        File countryRegionFile = new File(countryRegionCSV);
        // retrieve the migration queries for this data
        MigrationQuery[] countryRegionMigration = DataMigrationQueries.getCountryRegionMigrationQueries();

        // migrate countries and regions
        try {
            CSVIterator csv = new CSVIterator(countryRegionFile, ',');
            for (int i = 0; csv.hasNext(); i++) {
                Map<String, String> line = csv.next();
                GraknClient.Transaction tx = session.transaction().write();
                if (i % 1 == 0) {
                    System.out.printf("Loaded country-region data: %d, %s\n", i, line.get("ISO"));
                }
                for (MigrationQuery q : countryRegionMigration) {
                    doMigration(line, q, tx);
                }
                tx.commit();
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }
    }

    private static void loadAnimalTradeData(GraknClient.Session trainSession, GraknClient.Session testSession, double trainSplit) throws IOException {

        String tradeTermCodesFile = "/Users/joshua/Documents/grakn_examples/animal-trade/data/trade_terms.csv";
        String unitCodesFile = "/Users/joshua/Documents/grakn_examples/animal-trade/data/units.csv";
        String purposeCodesFile = "/Users/joshua/Documents/grakn_examples/animal-trade/data/purposes.csv";
        Map<String, String> tradeTermCodes = getCodesFromCsv(tradeTermCodesFile, ' ');
        Map<String, String> unitCodes = getCodesFromCsv(unitCodesFile, ' ');
        Map<String, String> purposeCodes = getCodesFromCsv(purposeCodesFile, ' ');

        MigrationQuery[] taxonomyMigration = DataMigrationQueries.getTaxonomyHierarchyMigrationQueries();
        MigrationQuery importMigration = DataMigrationQueries.getImportMigrationQuery();
        MigrationQuery exportMigration = DataMigrationQueries.getExportMigrationQuery();

        String citiesTradeCSV = "/Users/joshua/Documents/grakn_examples/animal-trade/data/CITIES_data.csv";
        File dataFile = new File(citiesTradeCSV);
        Random random = new Random();

        try {
            CSVIterator csv = new CSVIterator(dataFile, ',');
            for (int i = 0; csv.hasNext(); i++) {
                boolean loadIntoTrain = random.nextDouble() < trainSplit;

                if (i % 1 == 0) {
                    System.out.printf("Loaded import/export into %s: %d\n", loadIntoTrain ? "train" : "test", i);
                }

                Map<String, String> line = csv.next();

                GraknClient.Session session = loadIntoTrain ? trainSession : testSession;
                loadTaxonomyHierarchy(line, taxonomyMigration, session);

                // replace the various codes in the line with full names, if we have a mapping
                String unit = unitCodes.get(line.get("Unit"));
                if (unit != null) {
                    line.replace("Unit", unit);
                }

                String term = tradeTermCodes.get(line.get("Term"));
                if (term != null) {
                    line.replace("Term", term);
                }

                String purpose = purposeCodes.get(line.get("Purpose"));
                if (purpose != null) {
                    line.replace("Purpose", purpose);
                }

                // push the session down to avoid inserting same attribtue twice in once insert
                loadExchange(line, importMigration, exportMigration, session);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static Map<String, String> getCodesFromCsv(String resourcePath, char separator) {
        File csvFile = new File(resourcePath);
        Map<String, String> codes = new HashMap<>();
        try {
            CSVIterator csv = new CSVIterator(csvFile, separator);
            csv.forEachRemaining(line -> codes.put(line.get("Code"), line.get("Description")));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return codes;
    }

    private static void loadTaxonomyHierarchy(Map<String, String> line, MigrationQuery[] taxonomyMigration, GraknClient.Session session) {
        // insert singleton taxonomy hierarchy instances
        GraknClient.Transaction tx = session.transaction().write();
        for (MigrationQuery q : taxonomyMigration) {
            doMigration(line, q, tx);
        }
        tx.commit();
    }

    private static void loadExchange(Map<String, String> line, MigrationQuery importMigration,
                                     MigrationQuery exportMigration, GraknClient.Session session) {

        // if the "importer reported quantity" is non-empty string
        // add an import
        String imported = line.get("Importer reported quantity");
        List<ConceptMap> importMigrationResult = null;
        if (imported.length() > 0) {
            try (GraknClient.Transaction tx = session.transaction().write()) {
                importMigrationResult = doMigration(line, importMigration, tx);
                tx.commit();
            }
        }

        // if "exported reported quantity" is non-empty string, add an export
        String exported = line.get("Exporter reported quantity");
        if (exported.length() > 0) {
            Concept importConcept = null;
            Concept exportConcept = null;
            if (importMigrationResult != null && importMigrationResult.size() > 0) {
                try (GraknClient.Transaction tx = session.transaction().write()) {
                    List<ConceptMap> exportMigrationResult = doMigration(line, exportMigration, tx);
                    // add relationships between the import and export
                    importConcept = importMigrationResult.get(0).get("import");
                    exportConcept = exportMigrationResult.get(0).get("export");
                    tx.commit();
                }
                try (GraknClient.Transaction tx = session.transaction().write()) {
                    addRelationship(tx, "import-export-correspondence",
                            Arrays.asList("corresponding-import", "corresponding-export"),
                            Arrays.asList(importConcept, exportConcept));
                    tx.commit();
                }
            }
        }
    }

    private static List<ConceptMap> doMigration(Map<String, String> line, MigrationQuery query, GraknClient.Transaction tx) {
        boolean exists = false;
        if (query instanceof SingletonInsertMigrationQuery) {
            String checkExistenceQuery = ((SingletonInsertMigrationQuery) query).getCheckExistQuery(line);
            GraqlGet parsedQuery = Graql.parse(checkExistenceQuery).asGet();
            Stream<ConceptMap> response = tx.stream(parsedQuery);
            exists = response.findFirst().isPresent();
        }

        // if the data of the query does not exist, insert it
        // the boolean is used for SingletonInsertMigrationQuery
        if (!exists) {
            String migrationQuery = query.getQuery(line);
            GraqlInsert parsedQuery = Graql.parse(migrationQuery).asInsert();
            List<ConceptMap> response = tx.execute(parsedQuery);
            // for detecting missing country codes, remove later
            System.out.println(response.size());
            if (response.size() != 1) {
                System.out.println(migrationQuery);
            }
            return response;
        }
        return null;
    }

    private static List<ConceptMap> addRelationship(GraknClient.Transaction tx, String relationship, List<String> roles, List<Concept> concepts) {

        List<String> conceptIds = concepts.stream().map(concept -> concept.id().toString()).collect(Collectors.toList());
        String addRelationshipQuery = AddRelationshipQuery.getAddRelationshipQuery(relationship, roles, conceptIds);

        GraqlInsert query = Graql.parse(addRelationshipQuery).asInsert();
        return tx.execute(query);
    }
}




