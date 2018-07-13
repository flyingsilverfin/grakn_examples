package com.joshua;
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.remote.RemoteGrakn;
import ai.grakn.util.SimpleURI;
import ai.grakn.graql.admin.Answer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import com.joshua.CSVIterator;
import groovy.lang.Singleton;

/**
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException {

        try (GraknSession session = RemoteGrakn.session(new SimpleURI("localhost:48555"), Keyspace.of("animaltrade"))) {
            loadCountryRegions(session); // make two load datas: one for each file to load data from
            loadAnimalTradeData(session);

            // TODO analyze/use data
        }

    }

    private static void loadCountryRegions(GraknSession session) throws IOException {
        String countryRegionCSV = "/Users/joshua/Documents/AnimalTrade/animal-trade/data/country_region_mapping.csv";
        File countryRegionFile = new File(countryRegionCSV);
        // retrieve the migration queries for this data
        MigrationQuery[] countryRegionMigration = DataMigrationQueries.getCountryRegionMigrationQueries();

        // migrate countries and regions
        try {
            CSVIterator csv = new CSVIterator(countryRegionFile, ',');
            for(int i = 0; csv.hasNext(); i++ ) {
                Map<String, String> line = csv.next();
                try (GraknTx tx = session.open(GraknTxType.WRITE)) {
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

    private static void loadAnimalTradeData(GraknSession session) throws IOException {
        MigrationQuery[] taxonomyMigration = DataMigrationQueries.getTaxonomyHierarchyMigrationQueries();
        MigrationQuery importMigration = DataMigrationQueries.getImportMigrationQuery();
        MigrationQuery exportMigration = DataMigrationQueries.getExportMigrationQuery();

        String citiesTradeCSV = "/Users/joshua/Documents/AnimalTrade/animal-trade/data/CITIES_data.csv";
        File dataFile = new File(citiesTradeCSV);

        try {
            CSVIterator csv = new CSVIterator(dataFile, ',');
            for (int i = 0; csv.hasNext(); i++) {
                if (i % 1 == 0) {
                    System.out.printf("Loaded import/export: %d\n", i);
                }
                Map<String, String> line = csv.next();
                try (GraknTx tx = session.open(GraknTxType.WRITE)) {
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

    private static void loadTaxonomyHierarchy(Map<String, String> line, MigrationQuery[] taxonomyMigration, GraknTx tx) {
        // insert singleton taxonomy hierarchy instances
        for (MigrationQuery q : taxonomyMigration) {
            doMigration(line, q, tx);
        }
    }

    private static void loadExchange(Map<String, String> line, MigrationQuery importMigration,
                                     MigrationQuery exportMigration, GraknTx tx) {

        // if the "importer reported quantity" is non-empty string
        // add an import
        String imported = line.get("Importer reported quantity");
        List<Answer> importMigrationResult = null;
        if (imported.length() > 0) {
            importMigrationResult = doMigration(line, importMigration, tx);
        }

        // if "exported reported quantity" is non-empty string, add an export
        String exported =  line.get("Exporter reported quantity");
        if (exported.length() > 0) {
            List<Answer> exportMigrationResult = doMigration(line, exportMigration, tx);

            if (importMigrationResult != null && importMigrationResult.size() > 0) {
                // add relationships between the import and export
                Concept importConcept = Iterables.getOnlyElement(importMigrationResult).get("import");
                Concept exportConcept = Iterables.getOnlyElement(exportMigrationResult).get("export");

                // create and run the query to link these two concepts
                addRelationship(tx, "correspondence",
                        Arrays.asList("correspondence-import", "correspondence-export"),
                        Arrays.asList(importConcept, exportConcept));

            }
        }
    }



    private static List<Answer> doMigration(Map<String, String> line, MigrationQuery query, GraknTx tx) {
        QueryBuilder qb = tx.graql();
        boolean exists = false;
        if (query instanceof SingletonInsertMigrationQuery) {
            String checkExistenceQuery = ((SingletonInsertMigrationQuery) query).getCheckExistQuery(line);
            GetQuery parsedQuery= qb.parse(checkExistenceQuery);
            List<Answer> response = parsedQuery.execute();
            exists = response.size() != 0;
        }

        // if the data of the query does not exist, insert it
        // the boolean is used for SingletonInsertMigrationQuery
        if (!exists) {
            String migrationQuery = query.getQuery(line);
            InsertQuery parsedQuery = qb.parse(migrationQuery);
            List<Answer> response = parsedQuery.execute();
            // for detecting missing country codes, remove later
            System.out.println(response.size());
            if (response.size() != 1) {
                System.out.println(migrationQuery);
            }
            return response;
        }
        return null;
    }

    private static List<Answer> addRelationship(GraknTx tx, String relationship, List<String> roles, List<Concept> concepts) {

        List<String> conceptIds = concepts.stream().map(concept -> concept.getId().toString()).collect(Collectors.toList());
        String addRelationshipQuery = AddRelationshipQuery.getAddRelationshipQuery(relationship, roles, conceptIds);

        QueryBuilder qb = tx.graql();
        InsertQuery query = qb.parse(addRelationshipQuery);
        return query.execute();
    }
}




