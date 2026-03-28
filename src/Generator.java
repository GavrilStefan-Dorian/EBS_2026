import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Generator {

    private final int numPublications;
    private final int numSubscriptions;
    private final int parallelism;
    private final long seed;
    private final Path outputDir;

    public Generator(int numPublications, int numSubscriptions, int parallelism, long seed, String outputDir) {
        this.numPublications = numPublications;
        this.numSubscriptions = numSubscriptions;
        this.parallelism = Math.max(1, parallelism);
        this.seed = seed;
        this.outputDir = Path.of(outputDir);
    }

    public Map<String, Double> generateAll() throws IOException, ExecutionException, InterruptedException {
        Path publicationsPath = outputDir.resolve("publications.txt");
        Path subscriptionsPath = outputDir.resolve("subscriptions.txt");
        Path reportPath = outputDir.resolve("report.txt");

        long t0 = System.nanoTime();
        Map<String, Set<Integer>> presencePlan = buildPresencePlan(numSubscriptions, seed);
        Map<String, Set<Integer>> equalityPlan = buildEqualityPlan(numSubscriptions, presencePlan, seed);
        double planningTime = secondsSince(t0);

        long t1 = System.nanoTime();
        List<String> publications = generatePublicationsParallel();
        double publicationGenerationTime = secondsSince(t1);

        long t2 = System.nanoTime();
        List<String> subscriptions = generateSubscriptionsParallel(presencePlan, equalityPlan);
        double subscriptionGenerationTime = secondsSince(t2);

        long t3 = System.nanoTime();
        writeLines(publicationsPath, publications);
        writeLines(subscriptionsPath, subscriptions);
        double ioTime = secondsSince(t3);

        Map<String, Integer> presentCounts = countFieldPresence(subscriptions);
        Map<String, Integer> equalityCounts = countEqualityOperators(subscriptions);

        writeReport(
                reportPath,
                planningTime,
                publicationGenerationTime,
                subscriptionGenerationTime,
                ioTime,
                presentCounts,
                equalityCounts
        );

        Map<String, Double> timings = new HashMap<>();
        timings.put("planning_time", planningTime);
        timings.put("publication_generation_time", publicationGenerationTime);
        timings.put("subscription_generation_time", subscriptionGenerationTime);
        timings.put("io_time", ioTime);
        timings.put("total_time", planningTime + publicationGenerationTime + subscriptionGenerationTime + ioTime);

        return timings;
    }

    private List<String> generatePublicationsParallel() throws InterruptedException, ExecutionException {
        List<int[]> ranges = splitIntoRanges(numPublications, parallelism);
        ExecutorService executor = Executors.newFixedThreadPool(parallelism);
        List<Future<List<String>>> futures = new ArrayList<>();

        for (int[] range : ranges) {
            int start = range[0];
            int end = range[1];
            futures.add(executor.submit(() -> generatePublicationChunk(start, end)));
        }

        List<String> result = new ArrayList<>(numPublications);
        for (Future<List<String>> future : futures) {
            result.addAll(future.get());
        }

        executor.shutdown();
        return result;
    }

    private List<String> generateSubscriptionsParallel(Map<String, Set<Integer>> presencePlan,
                                                       Map<String, Set<Integer>> equalityPlan)
            throws InterruptedException, ExecutionException {
        List<int[]> ranges = splitIntoRanges(numSubscriptions, parallelism);
        ExecutorService executor = Executors.newFixedThreadPool(parallelism);
        List<Future<List<String>>> futures = new ArrayList<>();

        for (int[] range : ranges) {
            int start = range[0];
            int end = range[1];
            futures.add(executor.submit(() -> generateSubscriptionChunk(start, end, presencePlan, equalityPlan)));
        }

        List<String> result = new ArrayList<>(numSubscriptions);
        for (Future<List<String>> future : futures) {
            result.addAll(future.get());
        }

        executor.shutdown();
        return result;
    }

    private List<String> generatePublicationChunk(int start, int end) {
        List<String> lines = new ArrayList<>(end - start);
        for (int i = start; i < end; i++) {
            lines.add(generatePublicationLine(i));
        }
        return lines;
    }

    private List<String> generateSubscriptionChunk(int start, int end,
                                                   Map<String, Set<Integer>> presencePlan,
                                                   Map<String, Set<Integer>> equalityPlan) {
        List<String> lines = new ArrayList<>(end - start);
        for (int i = start; i < end; i++) {
            lines.add(generateSubscriptionLine(i, presencePlan, equalityPlan));
        }
        return lines;
    }

    private String generatePublicationLine(int index) {
        Random rng = new Random(seed + (long) index * 9973L);

        List<String> parts = new ArrayList<>();
        parts.add(formatPublicationField("company", "string", randomCompany(rng)));
        parts.add(formatPublicationField("value", "double", randomDouble(rng, 10.0, 500.0, 2)));
        parts.add(formatPublicationField("drop", "double", randomDouble(rng, 0.0, 40.0, 2)));
        parts.add(formatPublicationField("variation", "double", randomDouble(rng, -1.0, 1.0, 3)));
        parts.add(formatPublicationField("date", "date", randomDate(rng)));

        return "{" + String.join(";", parts) + "}";
    }

    private String generateSubscriptionLine(int index,
                                            Map<String, Set<Integer>> presencePlan,
                                            Map<String, Set<Integer>> equalityPlan) {
        Random rng = new Random(seed + (long) index * 12347L);
        List<String> parts = new ArrayList<>();

        for (String fieldName : Config.FIELD_ORDER) {
            if (!presencePlan.get(fieldName).contains(index)) {
                continue;
            }

            String operator = chooseOperator(fieldName, index, equalityPlan, rng);
            String kind = Config.FIELD_KINDS.get(fieldName);
            Object value = generateFieldValue(fieldName, rng);

            parts.add(formatSubscriptionField(fieldName, kind, operator, value));
        }

        return "{" + String.join(";", parts) + "}";
    }

    private Map<String, Set<Integer>> buildPresencePlan(int totalSubscriptions, long seed) {
        Random rng = new Random(seed);
        Map<String, Set<Integer>> plan = new HashMap<>();

        for (String fieldName : Config.FIELD_ORDER) {
            double percent = Config.FIELD_PRESENCE.get(fieldName);
            int count = exactCount(totalSubscriptions, percent);
            plan.put(fieldName, pickExactIndices(totalSubscriptions, count, rng));
        }

        return plan;
    }

    private Map<String, Set<Integer>> buildEqualityPlan(int totalSubscriptions,
                                                        Map<String, Set<Integer>> presencePlan,
                                                        long seed) {
        Random rng = new Random(seed + 100_000L);
        Map<String, Set<Integer>> plan = new HashMap<>();

        for (String fieldName : Config.EQUALITY_MIN_PERCENT.keySet()) {
            int requiredEq = exactCount(totalSubscriptions, Config.EQUALITY_MIN_PERCENT.get(fieldName));
            List<Integer> presentIndices = new ArrayList<>(presencePlan.get(fieldName));

            if (requiredEq > presentIndices.size()) {
                throw new IllegalArgumentException(
                        "Field '" + fieldName + "' requires " + requiredEq +
                                " equality cases but is present only " + presentIndices.size() + " times."
                );
            }

            Collections.shuffle(presentIndices, rng);
            plan.put(fieldName, new HashSet<>(presentIndices.subList(0, requiredEq)));
        }

        return plan;
    }

    private String chooseOperator(String fieldName, int subscriptionIndex,
                                  Map<String, Set<Integer>> equalityPlan, Random rng) {
        List<String> operators = Config.FIELD_OPERATORS.get(fieldName);

        if (equalityPlan.containsKey(fieldName) && equalityPlan.get(fieldName).contains(subscriptionIndex)) {
            return "=";
        }

        if (equalityPlan.containsKey(fieldName)) {
            List<String> nonEq = new ArrayList<>();
            for (String op : operators) {
                if (!op.equals("=")) {
                    nonEq.add(op);
                }
            }
            return nonEq.get(rng.nextInt(nonEq.size()));
        }

        return operators.get(rng.nextInt(operators.size()));
    }

    private Object generateFieldValue(String fieldName, Random rng) {
        return switch (fieldName) {
            case "company" -> randomCompany(rng);
            case "value" -> randomDouble(rng, 10.0, 500.0, 2);
            case "drop" -> randomDouble(rng, 0.0, 40.0, 2);
            case "variation" -> randomDouble(rng, -1.0, 1.0, 3);
            case "date" -> randomDate(rng);
            default -> throw new IllegalArgumentException("Unknown field: " + fieldName);
        };
    }

    private String randomCompany(Random rng) {
        return Config.COMPANIES.get(rng.nextInt(Config.COMPANIES.size()));
    }

    private double randomDouble(Random rng, double low, double high, int decimals) {
        double factor = Math.pow(10, decimals);
        double value = low + (high - low) * rng.nextDouble();
        return Math.round(value * factor) / factor;
    }

    private String randomDate(Random rng) {
        long days = Config.DATE_START.until(Config.DATE_END).getDays()
                + Config.DATE_START.until(Config.DATE_END).getMonths() * 30L
                + Config.DATE_START.until(Config.DATE_END).getYears() * 365L;

        long realDays = Config.DATE_END.toEpochDay() - Config.DATE_START.toEpochDay();
        long offset = rng.nextLong(realDays + 1);
        LocalDate picked = Config.DATE_START.plusDays(offset);

        return picked.format(DateTimeFormatter.ofPattern("dd.MM.yyyy"));
    }

    private String formatPublicationField(String name, String kind, Object value) {
        return "(" + name + "," + formatValue(kind, value) + ")";
    }

    private String formatSubscriptionField(String name, String kind, String operator, Object value) {
        return "(" + name + "," + operator + "," + formatValue(kind, value) + ")";
    }

    private String formatValue(String kind, Object value) {
        if ("string".equals(kind) || "date".equals(kind)) {
            return "\"" + value + "\"";
        }
        return String.valueOf(value);
    }

    private int exactCount(int total, double percent) {
        return (int) Math.round(total * percent / 100.0);
    }

    private Set<Integer> pickExactIndices(int total, int count, Random rng) {
        List<Integer> all = new ArrayList<>(total);
        for (int i = 0; i < total; i++) {
            all.add(i);
        }
        Collections.shuffle(all, rng);
        return new HashSet<>(all.subList(0, Math.min(count, total)));
    }

    private List<int[]> splitIntoRanges(int total, int workers) {
        List<int[]> ranges = new ArrayList<>();
        int chunkSize = (int) Math.ceil((double) total / workers);

        int start = 0;
        while (start < total) {
            int end = Math.min(total, start + chunkSize);
            ranges.add(new int[]{start, end});
            start = end;
        }

        return ranges;
    }

    private void writeLines(Path path, List<String> lines) throws IOException {
        Files.createDirectories(path.getParent());
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            for (String line : lines) {
                writer.write(line);
                writer.newLine();
            }
        }
    }

    private Map<String, Integer> countFieldPresence(List<String> subscriptions) {
        Map<String, Integer> counts = new HashMap<>();
        for (String fieldName : Config.FIELD_ORDER) {
            counts.put(fieldName, 0);
        }

        for (String line : subscriptions) {
            for (String fieldName : Config.FIELD_ORDER) {
                if (line.contains("(" + fieldName + ",")) {
                    counts.put(fieldName, counts.get(fieldName) + 1);
                }
            }
        }

        return counts;
    }

    private Map<String, Integer> countEqualityOperators(List<String> subscriptions) {
        Map<String, Integer> counts = new HashMap<>();
        for (String fieldName : Config.FIELD_ORDER) {
            counts.put(fieldName, 0);
        }

        for (String line : subscriptions) {
            for (String fieldName : Config.FIELD_ORDER) {
                if (line.contains("(" + fieldName + ",=,")) {
                    counts.put(fieldName, counts.get(fieldName) + 1);
                }
            }
        }

        return counts;
    }

    private void writeReport(Path reportPath,
                             double planningTime,
                             double publicationGenerationTime,
                             double subscriptionGenerationTime,
                             double ioTime,
                             Map<String, Integer> presentCounts,
                             Map<String, Integer> equalityCounts) throws IOException {
        Files.createDirectories(reportPath.getParent());

        try (BufferedWriter writer = Files.newBufferedWriter(reportPath)) {
            writer.write("Generation report");
            writer.newLine();
            writer.write("publications=" + numPublications);
            writer.newLine();
            writer.write("subscriptions=" + numSubscriptions);
            writer.newLine();
            writer.write("parallelism=" + parallelism);
            writer.newLine();
            writer.write(String.format(Locale.US, "planning_time=%.6fs%n", planningTime));
            writer.write(String.format(Locale.US, "publication_generation_time=%.6fs%n", publicationGenerationTime));
            writer.write(String.format(Locale.US, "subscription_generation_time=%.6fs%n", subscriptionGenerationTime));
            writer.write(String.format(Locale.US, "io_time=%.6fs%n", ioTime));
            writer.newLine();

            writer.write("Field presence counts:");
            writer.newLine();
            for (String fieldName : Config.FIELD_ORDER) {
                writer.write(fieldName + "=" + presentCounts.get(fieldName));
                writer.newLine();
            }

            writer.newLine();
            writer.write("Equality counts:");
            writer.newLine();
            for (String fieldName : Config.FIELD_ORDER) {
                writer.write(fieldName + "=" + equalityCounts.get(fieldName));
                writer.newLine();
            }
        }
    }

    private double secondsSince(long startNano) {
        return (System.nanoTime() - startNano) / 1_000_000_000.0;
    }
}