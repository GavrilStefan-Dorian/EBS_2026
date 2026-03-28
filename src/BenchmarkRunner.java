import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BenchmarkRunner {

    public static void main(String[] args) {
        List<ResultRow> results = new ArrayList<>();

        results.add(runCase(10_000, 10_000, 1, "output_10k_p1", 42L));
        results.add(runCase(10_000, 10_000, 4, "output_10k_p4", 42L));
        results.add(runCase(200_000, 200_000, 1, "output_200k_p1", 42L));
        results.add(runCase(200_000, 200_000, 4, "output_200k_p4", 42L));

        System.out.println("\n============== FINAL SUMMARY ==============");
        System.out.printf("%-12s %-14s %-12s %-12s%n", "Pubs", "Subs", "Threads", "Total Time");
        for (ResultRow row : results) {
            System.out.printf("%-12d %-14d %-12d %-12.6f%n",
                    row.publications, row.subscriptions, row.parallelism, row.totalTime);
        }
    }

    private static ResultRow runCase(int publications,
                                     int subscriptions,
                                     int parallelism,
                                     String outputDir,
                                     long seed) {
        System.out.println("==========================================");
        System.out.println("Running:");
        System.out.println("publications = " + publications);
        System.out.println("subscriptions = " + subscriptions);
        System.out.println("parallelism = " + parallelism);
        System.out.println("outputDir = " + outputDir);
        System.out.println("seed = " + seed);
        System.out.println("==========================================");

        Generator generator = new Generator(
                publications,
                subscriptions,
                parallelism,
                seed,
                outputDir
        );

        try {
            Map<String, Double> timings = generator.generateAll();

            System.out.println("Done.");
            for (Map.Entry<String, Double> entry : timings.entrySet()) {
                System.out.printf("%s: %.6fs%n", entry.getKey(), entry.getValue());
            }
            System.out.println("Files written to: " + outputDir);
            System.out.println();

            return new ResultRow(
                    publications,
                    subscriptions,
                    parallelism,
                    timings.get("total_time")
            );

        } catch (Exception e) {
            System.out.println("Benchmark case failed for outputDir = " + outputDir);
            e.printStackTrace();
            return new ResultRow(publications, subscriptions, parallelism, -1.0);
        }
    }

    private record ResultRow(int publications, int subscriptions, int parallelism, double totalTime) {
    }
}