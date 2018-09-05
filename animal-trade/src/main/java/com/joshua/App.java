package com.joshua;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.client.Grakn;
import ai.grakn.concept.Concept;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.answer.Answer;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.util.SimpleURI;
import com.google.common.collect.Iterables;

/**
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException {
        final String GRAKN_URI = "localhost:48555";
        final String GRAKN_KEYSPACE = "animaltrade";
        try (Grakn.Session session = new Grakn(new SimpleURI(GRAKN_URI)).session(Keyspace.of(GRAKN_KEYSPACE))) {
            loadCountryRegions(session); // make two load datas: one for each file to load data from
            loadAnimalTradeData(session);

            // TODO analyze/use data
        }

    }

    private static void loadCountryRegions(Grakn.Session session) throws IOException {
        String countryRegionCSV = "/Users/joshua/Documents/grakn_examples/animal-trade/data/country_region_mapping.csv";
        File countryRegionFile = new File(countryRegionCSV);
        // retrieve the migration queries for this data
        MigrationQuery[] countryRegionMigration = DataMigrationQueries.getCountryRegionMigrationQueries();

        // migrate countries and regions
        try {
            CSVIterator csv = new CSVIterator(countryRegionFile, ',');
            for(int i = 0; csv.hasNext(); i++ ) {
                Map<String, String> line = csv.next();
                try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
                    if (i % 1 == 0) {
                        System.out.printf("Loaded country-region data: %d, %s\n", i, line.get("ISO"));
                    }
                    for (MigrationQuery q : countryRegionMigration) {
                        doMigration(line, q, tx);
                    }
                    tx.commit();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }
    }

    private static void loadAnimalTradeData(Grakn.Session session) throws IOException {
        MigrationQuery[] taxonomyMigration = DataMigrationQueries.getTaxonomyHierarchyMigrationQueries();
        MigrationQuery importMigration = DataMigrationQueries.getImportMigrationQuery();
        MigrationQuery exportMigration = DataMigrationQueries.getExportMigrationQuery();

        String citiesTradeCSV = "/Users/joshua/Documents/grakn_examples/animal-trade/data/CITIES_data.csv";
        File dataFile = new File(citiesTradeCSV);

        try {
            CSVIterator csv = new CSVIterator(dataFile, ',');
            for (int i = 0; csv.hasNext(); i++) {
                if (i % 1 == 0) {
                    System.out.printf("Loaded import/export: %d\n", i);
                }
                Map<String, String> line = csv.next();
                try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
                    loadTaxonomyHierarchy(line, taxonomyMigration, tx);
                    loadExchange(line, importMigration, exportMigration, tx);
                    tx.commit();
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static void loadTaxonomyHierarchy(Map<String, String> line, MigrationQuery[] taxonomyMigration, Grakn.Transaction tx) {
        // insert singleton taxonomy hierarchy instances
        for (MigrationQuery q : taxonomyMigration) {
            doMigration(line, q, tx);
        }
    }

    private static void loadExchange(Map<String, String> line, MigrationQuery importMigration,
                                     MigrationQuery exportMigration, Grakn.Transaction tx) {

        // if the "importer reported quantity" is non-empty string
        // add an import
        String imported = line.get("Importer reported quantity");
        List<ConceptMap> importMigrationResult = null;
        if (imported.length() > 0) {
            importMigrationResult = doMigration(line, importMigration, tx);
        }

        // if "exported reported quantity" is non-empty string, add an export
        String exported =  line.get("Exporter reported quantity");
        if (exported.length() > 0) {
            List<ConceptMap> exportMigrationResult = doMigration(line, exportMigration, tx);

            if (importMigrationResult != null && importMigrationResult.size() > 0) {
                // add relationships between the import and export
                Concept importConcept = Iterables.getOnlyElement(importMigrationResult).asConceptMap().get("import");
                Concept exportConcept = Iterables.getOnlyElement(exportMigrationResult).asConceptMap().get("export");

                // create and run the query to link these two concepts
                addRelationship(tx, "correspondence",
                        Arrays.asList("correspondence-import", "correspondence-export"),
                        Arrays.asList(importConcept, exportConcept));

            }
        }
    }



    private static List<ConceptMap> doMigration(Map<String, String> line, MigrationQuery query, Grakn.Transaction tx) {
        QueryBuilder qb = tx.graql();
        boolean exists = false;
        if (query instanceof SingletonInsertMigrationQuery) {
            String checkExistenceQuery = ((SingletonInsertMigrationQuery) query).getCheckExistQuery(line);
            GetQuery parsedQuery= qb.parse(checkExistenceQuery);
            List<ConceptMap> response = parsedQuery.execute();
            exists = response.size() != 0;
        }

        // if the data of the query does not exist, insert it
        // the boolean is used for SingletonInsertMigrationQuery
        if (!exists) {
            String migrationQuery = query.getQuery(line);
            InsertQuery parsedQuery = qb.parse(migrationQuery);
            List<ConceptMap> response = parsedQuery.execute();
            // for detecting missing country codes, remove later
            System.out.println(response.size());
            if (response.size() != 1) {
                System.out.println(migrationQuery);
            }
            return response;
        }
        return null;
    }

    private static List<ConceptMap> addRelationship(Grakn.Transaction tx, String relationship, List<String> roles, List<Concept> concepts) {

        List<String> conceptIds = concepts.stream().map(concept -> concept.id().toString()).collect(Collectors.toList());
        String addRelationshipQuery = AddRelationshipQuery.getAddRelationshipQuery(relationship, roles, conceptIds);

        QueryBuilder qb = tx.graql();
        InsertQuery query = qb.parse(addRelationshipQuery);
        return query.execute();
    }
}




