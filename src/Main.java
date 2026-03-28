import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Main {

    public static void main(String[] args) {
        int publications = 10000;
        int subscriptions = 10000;
        int parallelism = 4;
        long seed = 42L;
        String outputDir = "output";

        if (args.length >= 1) {
            publications = Integer.parseInt(args[0]);
        }
        if (args.length >= 2) {
            subscriptions = Integer.parseInt(args[1]);
        }
        if (args.length >= 3) {
            parallelism = Integer.parseInt(args[2]);
        }
        if (args.length >= 4) {
            outputDir = args[3];
        }
        if (args.length >= 5) {
            seed = Long.parseLong(args[4]);
        }

        Generator generator = new Generator(publications, subscriptions, parallelism, seed, outputDir);

        try {
            Map<String, Double> timings = generator.generateAll();

            System.out.println("Done.");

            System.out.printf("planning_time: %.6fs%n", timings.get("planning_time"));
            System.out.printf("publication_generation_time: %.6fs%n", timings.get("publication_generation_time"));
            System.out.printf("subscription_generation_time: %.6fs%n", timings.get("subscription_generation_time"));
            System.out.printf("io_time: %.6fs%n", timings.get("io_time"));
            System.out.printf("total_time: %.6fs%n", timings.get("total_time"));

            System.out.println("Files written to: " + outputDir);

        } catch (IOException | ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}