package lakewire.cost;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/** AWS list prices loaded from a properties file. */
public final class CostPrices {

    public static final String DEFAULT_RESOURCE = "/prices/aws-us-east-1.properties";

    private final BigDecimal interAzPerGb;
    private final BigDecimal s3PutPerThousand;
    private final BigDecimal s3GetPerThousand;
    private final BigDecimal s3GbMonth;
    private final BigDecimal ebsGbMonth;

    public CostPrices(
        BigDecimal interAzPerGb,
        BigDecimal s3PutPerThousand,
        BigDecimal s3GetPerThousand,
        BigDecimal s3GbMonth,
        BigDecimal ebsGbMonth
    ) {
        this.interAzPerGb = interAzPerGb;
        this.s3PutPerThousand = s3PutPerThousand;
        this.s3GetPerThousand = s3GetPerThousand;
        this.s3GbMonth = s3GbMonth;
        this.ebsGbMonth = ebsGbMonth;
    }

    public static CostPrices defaults() {
        try (InputStream in = CostPrices.class.getResourceAsStream(DEFAULT_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("missing " + DEFAULT_RESOURCE);
            }
            return load(in);
        } catch (IOException failure) {
            throw new IllegalStateException("cannot read default prices", failure);
        }
    }

    public static CostPrices fromFile(Path path) {
        try (Reader reader = Files.newBufferedReader(path)) {
            Properties properties = new Properties();
            properties.load(reader);
            return from(properties);
        } catch (IOException failure) {
            throw new IllegalStateException("cannot read " + path, failure);
        }
    }

    static CostPrices load(InputStream in) throws IOException {
        Properties properties = new Properties();
        properties.load(in);
        return from(properties);
    }

    static CostPrices from(Properties properties) {
        return new CostPrices(
            decimal(properties, "inter.az.gb"),
            decimal(properties, "s3.put.per.1000"),
            decimal(properties, "s3.get.per.1000"),
            decimal(properties, "s3.gb.month"),
            decimal(properties, "ebs.gb.month")
        );
    }

    public BigDecimal interAzPerGb() {
        return interAzPerGb;
    }

    public BigDecimal s3PutPerThousand() {
        return s3PutPerThousand;
    }

    public BigDecimal s3GetPerThousand() {
        return s3GetPerThousand;
    }

    public BigDecimal s3GbMonth() {
        return s3GbMonth;
    }

    public BigDecimal ebsGbMonth() {
        return ebsGbMonth;
    }

    private static BigDecimal decimal(Properties properties, String key) {
        String raw = properties.getProperty(key);
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("missing price " + key);
        }
        return new BigDecimal(raw.trim());
    }
}
