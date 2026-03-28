import java.time.LocalDate;
import java.util.List;
import java.util.Map;

public class Config {
    public static final List<String> COMPANIES = List.of(
            "Google", "Apple", "Amazon", "Meta", "Microsoft", "NVIDIA"
    );

    public static final LocalDate DATE_START = LocalDate.of(2022, 1, 1);
    public static final LocalDate DATE_END = LocalDate.of(2024, 12, 31);

    public static final Map<String, String> FIELD_KINDS = Map.of(
            "company", "string",
            "value", "double",
            "drop", "double",
            "variation", "double",
            "date", "date"
    );

    public static final Map<String, Double> FIELD_PRESENCE = Map.of(
            "company", 90.0,
            "value", 75.0,
            "drop", 55.0,
            "variation", 60.0,
            "date", 35.0
    );

    public static final Map<String, List<String>> FIELD_OPERATORS = Map.of(
            "company", List.of("=", "!="),
            "value", List.of("=", "<", "<=", ">", ">="),
            "drop", List.of("=", "<", "<=", ">", ">="),
            "variation", List.of("=", "<", "<=", ">", ">="),
            "date", List.of("=", "<", "<=", ">", ">=")
    );

    public static final Map<String, Double> EQUALITY_MIN_PERCENT = Map.of(
            "company", 70.0
    );

    public static final List<String> FIELD_ORDER = List.of(
            "company", "value", "drop", "variation", "date"
    );
}