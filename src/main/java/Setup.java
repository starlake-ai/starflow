/**
 * Setup.java
 *
 * This class provides a comprehensive setup utility for downloading, configuring, and managing
 * dependencies required for the Starlake project and its supported platforms (e.g., Spark, BigQuery,
 * Azure, Snowflake, Redshift, PostgreSQL, DuckDB, Kafka, etc.). It supports both Unix and Windows
 * environments and can interactively prompt the user to select which dependencies to enable.
 *
 * <p>
 * Main Features:
 * <ul>
 *   <li>Parses environment variables to determine which dependencies and features to enable.</li>
 *   <li>Supports proxy configuration (HTTP, HTTPS, SOCKS) with optional authentication.</li>
 *   <li>Downloads required JARs, binaries, and Python libraries from remote repositories, displaying
 *       progress and handling redirects and SSL settings.</li>
 *   <li>Handles extraction of archives (ZIP, TGZ) and manages file system operations (deletion,
 *       renaming, directory creation).</li>
 *   <li>Generates version files for both Unix (versions.sh) and Windows (versions.cmd) with all
 *       relevant dependency versions and feature flags.</li>
 *   <li>Provides interactive CLI menus for dependency selection, supporting both Unix (with arrow
 *       keys and terminal control) and Windows (text-based input).</li>
 *   <li>Implements custom TrustManager and ProxySelector for advanced HTTP client configuration.</li>
 *   <li>Updates Spark log4j2 configuration for consistent logging behavior.</li>
 * </ul>
 *
 * <p>
 * Usage:
 * <pre>
 *   java Setup &lt;target-directory&gt; [unix]
 * </pre>
 * If no dependencies are enabled via environment variables, the user will be prompted to select them interactively.
 *
 * <p>
 * Key Classes and Methods:
 * <ul>
 *   <li>{@code ResourceDependency}: Represents a downloadable resource (JAR, binary, etc.) with one or more URLs.</li>
 *   <li>{@code setProxy()}, {@code parseProxy(String)}: Configure proxy settings from environment variables.</li>
 *   <li>{@code setHttpClient()}: Configures the HTTP client with proxy, authentication, and SSL settings.</li>
 *   <li>{@code downloadAndDisplayProgress(...)}: Downloads resources with progress indication.</li>
 *   <li>{@code generateVersions(...)}: Writes version/configuration files for the selected environment.</li>
 *   <li>{@code askUserWhichConfigToEnable()}: Interactive menu for dependency selection.</li>
 *   <li>{@code main(String[] args)}: Entry point; orchestrates the setup process.</li>
 * </ul>
 *
 * <p>
 * Environment Variables:
 * <ul>
 *   <li>ENABLE_ALL, ENABLE_BIGQUERY, ENABLE_AZURE, ENABLE_SNOWFLAKE, ENABLE_REDSHIFT, ENABLE_POSTGRESQL, ENABLE_DUCKDB, ENABLE_KAFKA, ENABLE_MARIADB, ENABLE_CLICKHOUSE, ENABLE_TRINODB, ENABLE_SPARK</li>
 *   <li>SL_VERSION, SCALA_VERSION, SPARK_VERSION, HADOOP_VERSION, etc. (for version overrides)</li>
 *   <li>https_proxy, http_proxy, no_proxy (for proxy configuration)</li>
 *   <li>SL_INSECURE (to disable SSL certificate validation)</li>
 * </ul>
 *
 * <p>
 * Platform Support:
 * <ul>
 *   <li>Unix: Uses terminal control sequences for interactive menus and shell commands for terminal mode changes.</li>
 *   <li>Windows: Uses text-based menus and standard input for selection.</li>
 * </ul>
 *
 * <p>
 * Security:
 * <ul>
 *   <li>Supports disabling SSL certificate validation for proxies (not recommended for production).</li>
 *   <li>Handles proxy authentication via environment variables.</li>
 * </ul>
 *
 * <p>
 * Note:
 * <ul>
 *   <li>This class is intended to be run as a standalone setup utility and is not thread-safe.</li>
 *   <li>Some features (e.g., terminal raw/cooked mode) may not work in all environments or IDEs.</li>
 * </ul>
 *
 * @author Starlake contributors
 */
import javax.net.ssl.*;
import java.io.*;
import java.net.*;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class Setup extends ProxySelector implements X509TrustManager {

    private static class  UserPwdAuth extends  Authenticator {
        @Override
        protected PasswordAuthentication getPasswordAuthentication() {
            return new PasswordAuthentication(username, password.toCharArray());
        }
    };

    private static class ResourceDependency {

        private final String[] urls;

        private final String artefactName;

        public ResourceDependency(String artefactName, String... url) {
            this.urls = url;
            this.artefactName = artefactName;
        }

        public List<String> getUrlNames() {
            return Arrays.stream(urls).map(this::getUrlName).collect(Collectors.toList());
        }

        public String getUrlName(String url) {
            String name = url.substring(url.lastIndexOf("/") + 1);
            int q = name.indexOf("?");
            return q >= 0 ? name.substring(0, q) : name;
        }
    }

    private static String protocol = null;
    private static String host = null;
    private static int port = 0;
    private static String username = null;
    private static String password = null;

    private static String httpsProxy = getEnv("https_proxy").orElse("");
    private static String httpProxy = getEnv("http_proxy").orElse("");
    private static String noProxy = getEnv("no_proxy").orElse("").replaceAll(",", "|");
    private static Proxy proxy = Proxy.NO_PROXY;
    private static HttpClient.Builder clientBuilder = HttpClient.newBuilder();
    private static HttpClient client = null;

    private static boolean isWindowsOs() {
        String os = System.getProperty("os.name").toLowerCase();
        return os.startsWith("windows");
    }

    private static void parseProxy(String proxy) {
        if (proxy.isEmpty()) {
            return;
        }
        final Pattern pattern = Pattern.compile("(https?|socks5?):\\/\\/([^:].+)", Pattern.CASE_INSENSITIVE);
        final Matcher m = pattern.matcher(proxy);
        if (m.matches()) {
            protocol = m.group(1).toLowerCase();
            final String hostAndPortWithMaybeCredentials = m.group(2);
            if (hostAndPortWithMaybeCredentials.contains("@")) {
                final String[] hostAndPortWithCredentials = hostAndPortWithMaybeCredentials.split("@");
                final String[] credentials = hostAndPortWithCredentials[0].split(":");
                assert(credentials.length == 2): "Invalid credentials format, expecting 'username:password'";
                username = credentials[0];
                password = credentials[1];
                final String[] hostAndPort = hostAndPortWithCredentials[1].split(":");
                host = hostAndPort[0];
                if (hostAndPort.length > 1) {
                    port = Integer.parseInt(hostAndPort[1]);
                }
            } else {
                final String[] hostAndPort = hostAndPortWithMaybeCredentials.split(":");
                host = hostAndPort[0];
                if (hostAndPort.length > 1) {
                    port = Integer.parseInt(hostAndPort[1]);
                }
            }
        } else {
            throw new IllegalArgumentException("Invalid proxy format: " + proxy);
        }
    }

    private static void setProxy() {
        if (!httpsProxy.isEmpty()) {
            parseProxy(httpsProxy);
        } else if (!httpProxy.isEmpty()) {
            parseProxy(httpProxy);
        }
        if (host != null) {
            if (port == 0) {
                if (protocol.equals("https")) {
                    port = 443;
                } else if (protocol.startsWith("socks")) {
                    port = 1080;
                } else {
                    port = 80;
                }
            }
            Proxy.Type proxyType = Proxy.Type.HTTP;
            if (protocol.startsWith("socks")) {
                proxyType = Proxy.Type.SOCKS;
            }
            proxy = new Proxy(proxyType, new InetSocketAddress(host, port));
        }
        if (!noProxy.isEmpty()) {
            System.setProperty("http.nonProxyHosts", noProxy);
        }
    }

    // ENV VARS
    public static boolean ENABLE_ALL = envIsTrueWithDefaultTrue("ENABLE_ALL");
    public static boolean ENABLE_BIGQUERY = ENABLE_ALL || envIsTrueWithDefaultTrue("ENABLE_BIGQUERY");
    public static boolean ENABLE_AZURE = ENABLE_ALL || envIsTrueWithDefaultTrue("ENABLE_AZURE");
    public static boolean ENABLE_SNOWFLAKE = ENABLE_ALL || envIsTrueWithDefaultTrue("ENABLE_SNOWFLAKE");
    public static boolean ENABLE_REDSHIFT = ENABLE_ALL || envIsTrueWithDefaultTrue("ENABLE_REDSHIFT");
    public static boolean ENABLE_POSTGRESQL = ENABLE_ALL || envIsTrueWithDefaultTrue("ENABLE_POSTGRESQL");
    public static boolean ENABLE_DUCKDB = ENABLE_ALL || envIsTrueWithDefaultTrue("ENABLE_DUCKDB");
    public static boolean ENABLE_KAFKA = ENABLE_ALL || envIsTrueWithDefaultTrue("ENABLE_KAFKA");
    public static boolean ENABLE_MARIADB = ENABLE_ALL || envIsTrueWithDefaultTrue("ENABLE_MARIA");
    public static boolean ENABLE_CLICKHOUSE = ENABLE_ALL || envIsTrueWithDefaultTrue("ENABLE_CLICKHOUSE");
    public static boolean ENABLE_TRINODB = ENABLE_ALL || envIsTrueWithDefaultTrue("ENABLE_TRINODB");
    public static boolean ENABLE_SPARK = ENABLE_ALL;

    private static boolean[] getAllEnablers() {
        return new boolean[] {
            ENABLE_BIGQUERY,
            ENABLE_AZURE,
            ENABLE_SNOWFLAKE,
            ENABLE_REDSHIFT,
            ENABLE_POSTGRESQL,
            ENABLE_TRINODB,
            ENABLE_DUCKDB,
            ENABLE_KAFKA
        };
    }

    private static final boolean ENABLE_API = envIsTrueWithDefaultTrue("ENABLE_API");

    private static final String SCALA_VERSION = getEnv("SCALA_VERSION").orElse("2.13");

    // STARLAKE — SL_VERSION is the single source of truth; SL_API_VERSION defaults to it
    // If SL_VERSION env var is not set, fetch the latest release from Maven Central
    private static final String SL_VERSION = getEnv("SL_VERSION").orElse(getLatestMavenRelease());
    private static final String SL_API_VERSION = getEnv("SL_API_VERSION").orElse(SL_VERSION);

    /**
     * Fetch the latest released version of starlake-core from Maven Central metadata.
     * Falls back to the compile-time version if the fetch fails.
     */
    private static String getLatestMavenRelease() {
        try {
            String metadataUrl = "https://repo1.maven.org/maven2/ai/starlake/starlake-core_"
                    + SCALA_VERSION + "/maven-metadata.xml";
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection)
                    new java.net.URL(metadataUrl).openConnection();
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    // Parse: <release>1.5.7</release>
                    line = line.trim();
                    if (line.startsWith("<release>") && line.endsWith("</release>")) {
                        String version = line.substring("<release>".length(), line.length() - "</release>".length());
                        System.out.println("Latest starlake release on Maven Central: " + version);
                        return version;
                    }
                }
            } finally {
                conn.disconnect();
            }
        } catch (Exception e) {
            System.err.println("Could not fetch latest version from Maven Central: " + e.getMessage());
            System.err.println("Please set the SL_VERSION environment variable manually.");
        }
        return null;
    }

    // SPARK
    private static final String SPARK_VERSION = getEnv("SPARK_VERSION").orElse("3.5.8");
    private static final String HADOOP_VERSION = getEnv("HADOOP_VERSION").orElse("3");


    // BIGQUERY
    private static final String SPARK_BQ_VERSION = getEnv("SPARK_BQ_VERSION").orElse("0.44.0");

    // deltalake
    private static final String DELTA_SPARK = getEnv("SPARK_DELTA").orElse("3.3.2");

    // ICEBERG
    private static final String ICEBERG_SPARK = getEnv("SPARK_ICEBERG").orElse("1.10.0");

    private static final String HADOOP_AZURE_VERSION = getEnv("HADOOP_AZURE_VERSION").orElse("3.3.5");
    private static final String AZURE_STORAGE_VERSION = getEnv("AZURE_STORAGE_VERSION").orElse("8.6.6");
    private static final String JETTY_VERSION = getEnv("JETTY_VERSION").orElse("9.4.58.v20250814");

    // HADOOP_LIB ON WINDOWS
    private static final ResourceDependency[] HADOOP_LIBS = new ResourceDependency[]{
            new ResourceDependency("winutils", "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin/winutils.exe"),
            new ResourceDependency("hadoop.dll", "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin/hadoop.dll")
    };

    // SNOWFLAKE
    private static final String SNOWFLAKE_JDBC_VERSION = getEnv("SNOWFLAKE_JDBC_VERSION").orElse("3.28.0");
    private static final String SPARK_SNOWFLAKE_VERSION = getEnv("SPARK_SNOWFLAKE_VERSION").orElse("3.1.8");

    // POSTGRESQL
    private static final String POSTGRESQL_VERSION = getEnv("POSTGRESQL_VERSION").orElse("42.7.10");

    // MARIADB
    private static final String MARIADB_VERSION = getEnv("MARIADB_VERSION").orElse("3.5.4");

    // CLICKHOUSE
    private static final String CLICKHOUSE_VERSION = getEnv("CLICKHOUSE_VERSION").orElse("0.9.0");


    // TRINO
    private static final String TRINODB_VERSION = getEnv("TRINODB_VERSION").orElse("478");

    // DUCKDB
    private static final String DUCKDB_VERSION = getEnv("DUCKDB_VERSION").orElse("1.5.3.0");

    // REDSHIFT
    private static final String AWS_JAVA_SDK_VERSION = getEnv("AWS_JAVA_SDK_VERSION").orElse("1.12.797");
    private static final String HADOOP_AWS_VERSION = getEnv("HADOOP_AWS_VERSION").orElse("3.3.6");
    private static final String REDSHIFT_JDBC_VERSION = getEnv("REDSHIFT_JDBC_VERSION").orElse("2.1.0.34");
    private static final String SPARK_REDSHIFT_VERSION = getEnv("SPARK_REDSHIFT_VERSION").orElse("6.5.1");

    // KAFKA CONFLUENT
    private static final String CONFLUENT_VERSION = getEnv("CONFLUENT_VERSION").orElse("7.7.2");
    
    private static final String PYTHON_LIBS_URL = "https://raw.githubusercontent.com/starlake-ai/starlake/master/distrib/python-libs/";

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    // API — use snapshots repo for SNAPSHOT versions, Maven Central for releases
    private static final ResourceDependency SL_API_SNAPSHOT_ZIP = new ResourceDependency("starlake-api", "https://central.sonatype.com/repository/maven-snapshots/ai/starlake/starlake-api" + "_" + SCALA_VERSION + "/" + SL_API_VERSION + "/starlake-api"+ "_" + SCALA_VERSION + "-" + SL_API_VERSION + ".zip");
    private static final ResourceDependency SL_API_RELEASE_ZIP = new ResourceDependency("starlake-api", "https://repo1.maven.org/maven2/ai/starlake/starlake-api" + "_" + SCALA_VERSION + "/" + SL_API_VERSION + "/starlake-api" + "_" + SCALA_VERSION + "-" + SL_API_VERSION + ".zip");

    // SPARK
    private static final ResourceDependency SPARK_JAR = new ResourceDependency("dist/spark", "https://www.apache.org/dyn/closer.lua/spark/spark-" + SPARK_VERSION + "/spark-" + SPARK_VERSION + "-bin-hadoop" + HADOOP_VERSION  + "-scala2.13.tgz?action=download");
    private static final ResourceDependency SPARK_BQ_JAR = new ResourceDependency("bigquery-with-dependencies",
            "https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_" + SCALA_VERSION + "/" +
                    SPARK_BQ_VERSION + "/" +
                    "spark-bigquery-with-dependencies_" + SCALA_VERSION + "-" + SPARK_BQ_VERSION + ".jar");

    private static final ResourceDependency DELTA_SPARK_JAR = new ResourceDependency("delta-spark",
            "https://repo1.maven.org/maven2/io/delta/delta-spark_" + SCALA_VERSION + "/" + DELTA_SPARK + "/delta-spark_" + SCALA_VERSION + "-" + DELTA_SPARK + ".jar");

    private static final ResourceDependency ICEBERG_SPARK_JAR = new ResourceDependency("iceberg-spark",
            "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_" + SCALA_VERSION + "/" + ICEBERG_SPARK + "/iceberg-spark-runtime-3.5_" + SCALA_VERSION + "-" + ICEBERG_SPARK + ".jar");

    private static final ResourceDependency DELTA_STORAGE_JAR = new ResourceDependency("delta-storage",
            "https://repo1.maven.org/maven2/io/delta/delta-storage" + "/" + DELTA_SPARK + "/delta-storage" +"-" + DELTA_SPARK + ".jar");
    private static final ResourceDependency HADOOP_AZURE_JAR = new ResourceDependency("hadoop-azure", "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/" + HADOOP_AZURE_VERSION + "/hadoop-azure-" + HADOOP_AZURE_VERSION + ".jar");
    private static final ResourceDependency AZURE_STORAGE_JAR = new ResourceDependency("azure-storage", "https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/" + AZURE_STORAGE_VERSION + "/azure-storage-" + AZURE_STORAGE_VERSION + ".jar");
    private static final ResourceDependency JETTY_SERVER_JAR = new ResourceDependency("jetty-server", "https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-server/" + JETTY_VERSION + "/jetty-server-" + JETTY_VERSION + ".jar");
    private static final ResourceDependency SNOWFLAKE_JDBC_JAR = new ResourceDependency("snowflake-jdbc", "https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/" + SNOWFLAKE_JDBC_VERSION + "/snowflake-jdbc-" + SNOWFLAKE_JDBC_VERSION + ".jar");
    private static final ResourceDependency SPARK_SNOWFLAKE_JAR = new ResourceDependency("spark-snowflake", "https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_" + SCALA_VERSION +
            "/" + SPARK_SNOWFLAKE_VERSION + "/spark-snowflake_" + SCALA_VERSION + "-" + SPARK_SNOWFLAKE_VERSION + ".jar");
    private static final ResourceDependency POSTGRESQL_JAR = new ResourceDependency("postgresql", "https://repo1.maven.org/maven2/org/postgresql/postgresql/" + POSTGRESQL_VERSION + "/postgresql-" + POSTGRESQL_VERSION + ".jar");

    private static final ResourceDependency DUCKDB_JAR = new ResourceDependency("duckdb_jdbc", "https://repo1.maven.org/maven2/org/duckdb/duckdb_jdbc/" + DUCKDB_VERSION + "/duckdb_jdbc-" + DUCKDB_VERSION + ".jar");
    private static final ResourceDependency AWS_JAVA_SDK_JAR = new ResourceDependency("aws-java-sdk-bundle", "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/" + AWS_JAVA_SDK_VERSION + "/aws-java-sdk-bundle-" + AWS_JAVA_SDK_VERSION + ".jar");
    private static final ResourceDependency HADOOP_AWS_JAR = new ResourceDependency("hadoop-aws", "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/" + HADOOP_AWS_VERSION + "/hadoop-aws-" + HADOOP_AWS_VERSION + ".jar");
    private static final ResourceDependency REDSHIFT_JDBC_JAR = new ResourceDependency("redshift-jdbc42", "https://repo1.maven.org/maven2/com/amazon/redshift/redshift-jdbc42/" + REDSHIFT_JDBC_VERSION + "/redshift-jdbc42-" + REDSHIFT_JDBC_VERSION + ".jar");
    private static final ResourceDependency SPARK_REDSHIFT_JAR = new ResourceDependency("spark-redshift", "https://repo1.maven.org/maven2/ai/starlake/spark-redshift_" + SCALA_VERSION + "/" + SPARK_REDSHIFT_VERSION + "/spark-redshift_" + SCALA_VERSION + "-" + SPARK_REDSHIFT_VERSION + ".jar");

    private static final ResourceDependency STARLAKE_SNAPSHOT_JAR = new ResourceDependency("starlake-core", "https://central.sonatype.com/repository/maven-snapshots/ai/starlake/starlake-core" + "_" + SCALA_VERSION + "/" + SL_VERSION + "/starlake-core"+ "_" + SCALA_VERSION + "-" + SL_VERSION + "-assembly.jar");
    private static final ResourceDependency STARLAKE_RELEASE_JAR = new ResourceDependency("starlake-core", "https://repo1.maven.org/maven2/ai/starlake/starlake-core" + "_" + SCALA_VERSION + "/" + SL_VERSION + "/starlake-core" + "_" + SCALA_VERSION + "-" + SL_VERSION + "-assembly.jar");
    private static final ResourceDependency CONFLUENT_KAFKA_SCHEMA_REGISTRY_CLIENT = new ResourceDependency("kafka-schema-registry-client", "https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/" + CONFLUENT_VERSION + "/kafka-schema-registry-client-" + CONFLUENT_VERSION + ".jar");
    private static final ResourceDependency CONFLUENT_KAFKA_AVRO_SERIALIZER = new ResourceDependency("kafka-avro-serializer", "https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/" + CONFLUENT_VERSION + "/kafka-avro-serializer-" + CONFLUENT_VERSION + ".jar");

    private static final ResourceDependency MARIADB_JAR = new ResourceDependency("mariadb-java-client", "https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/" + MARIADB_VERSION + "/mariadb-java-client-" + MARIADB_VERSION + ".jar");
    private static final ResourceDependency CLICKHOUSE_JAR = new ResourceDependency("clickhouse-jdbc", "https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/"+ CLICKHOUSE_VERSION + "/clickhouse-jdbc-" + CLICKHOUSE_VERSION + "-all.jar");

    private static final ResourceDependency TRINODB_JAR = new ResourceDependency("trino-jdbc", "https://repo1.maven.org/maven2/io/trino/trino-jdbc/" + TRINODB_VERSION + "/trino-jdbc-" + TRINODB_VERSION + ".jar");


    private static final ResourceDependency[] snowflakeDependencies = {
            SNOWFLAKE_JDBC_JAR,
            SPARK_SNOWFLAKE_JAR
    };

    private static final ResourceDependency[] redshiftDependencies = {
            AWS_JAVA_SDK_JAR,
            HADOOP_AWS_JAR,
            REDSHIFT_JDBC_JAR,
            SPARK_REDSHIFT_JAR
    };

    private static final ResourceDependency[] azureDependencies = {
            HADOOP_AZURE_JAR,
            AZURE_STORAGE_JAR,
            JETTY_SERVER_JAR
    };

    private static final ResourceDependency[] postgresqlDependencies = {
            POSTGRESQL_JAR
    };

    private static final ResourceDependency[] duckDbDependencies = {
            DUCKDB_JAR
    };

    private static final ResourceDependency[] bigqueryDependencies = {
            SPARK_BQ_JAR
    };

    private static final ResourceDependency[] deltaSparkDependencies = {
            DELTA_SPARK_JAR,
            DELTA_STORAGE_JAR
    };

    private static final ResourceDependency[] icebergSparkDependencies = {
            ICEBERG_SPARK_JAR
    };

    private static final ResourceDependency[] confluentDependencies = {
            CONFLUENT_KAFKA_SCHEMA_REGISTRY_CLIENT,
            CONFLUENT_KAFKA_AVRO_SERIALIZER
    };
    private static final ResourceDependency[] mariadbDependencies = {
            MARIADB_JAR
    };
    private static final ResourceDependency[] clickhouseDependencies = {
    //        CLICKHOUSE_JAR
    };
    private static final ResourceDependency[] trinodbDependencies = {
            TRINODB_JAR
    };

    private static Optional<String> getEnv(String env) {
        // consider empty env variables as not set
        return Optional.ofNullable(System.getenv(env)).filter(s -> !s.isEmpty());
    }

    private static boolean envIsTrue(String env) {
        String value = getEnv(env).orElse("false");
        return !value.equals("false") && !value.equals("0");

    }

    private static boolean envIsTrueWithDefaultTrue(String env) {
        String value = getEnv(env).orElse("true");
        return !value.equals("false") && !value.equals("0");

    }

    private static void generateUnixVersions(File targetDir) throws IOException {
        generateVersions(targetDir, "versions.sh", "#!/bin/bash\nset -e\n\n",
                (writer) -> (variableName, value) -> {
                    try {
                        writer.write(variableName + "=" + "${" + variableName + ":-" + value + "}\n");
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private static void generateWindowsVersions(File targetDir) throws IOException {
        generateVersions(targetDir, "versions.cmd", "@ECHO OFF\n\n",
                (writer) -> (variableName, value) -> {
                    try {
                        writer.write(
                                "if \"%" + variableName + "%\"==\"\" (\n" +
                                        "    SET " + variableName + "=" + value + "\n" +
                                        ")\n");
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    // Used BiConsumer with Function because TriConsumer doesn't exist natively and avoid creating a new type
    /**
     * Generates a version file containing various dependency version variables and configuration flags.
     * <p>
     * This method creates a new file at the specified location, writes a header, and then populates
     * the file with a list of key-value pairs representing enabled features (e.g., BigQuery, Azure, Snowflake)
     * and library versions (e.g., Spark, HADOOP, Scala).
     * <p>
     * Certain versions are written conditionally based on whether specific features are enabled
     * or if no specific dependencies are enabled (providing a default set).
     *
     * @param targetDir        The directory where the versions file will be created.
     * @param versionsFileName The name of the file to be created.
     * @param fileHeader       A string header to be written at the beginning of the file.
     * @param variableWriter   A function that takes the BufferedWriter and returns a BiConsumer used
     *                         to write the key-value pairs to the file in a specific format.
     * @throws IOException If an I/O error occurs while writing to the file.
     */
    private static void generateVersions(File targetDir, String versionsFileName, String fileHeader, Function<BufferedWriter, BiConsumer<String, String>> variableWriter) throws IOException {
        File versionFile = new File(targetDir, versionsFileName);
        deleteFile(versionFile);
        BufferedWriter writer = new BufferedWriter(new FileWriter(versionFile));
        try {
            writer.write(fileHeader);
            variableWriter.apply(writer).accept("ENABLE_BIGQUERY", String.valueOf(ENABLE_BIGQUERY));
            variableWriter.apply(writer).accept("ENABLE_AZURE", String.valueOf(ENABLE_AZURE));
            variableWriter.apply(writer).accept("ENABLE_SNOWFLAKE", String.valueOf(ENABLE_SNOWFLAKE));
            variableWriter.apply(writer).accept("ENABLE_POSTGRESQL", String.valueOf(ENABLE_POSTGRESQL));
            variableWriter.apply(writer).accept("ENABLE_MARIADB", String.valueOf(ENABLE_MARIADB));
            variableWriter.apply(writer).accept("ENABLE_REDSHIFT", String.valueOf(ENABLE_REDSHIFT));
            variableWriter.apply(writer).accept("ENABLE_TRINODB", String.valueOf(ENABLE_TRINODB));
            variableWriter.apply(writer).accept("ENABLE_KAFKA", String.valueOf(ENABLE_KAFKA));
            variableWriter.apply(writer).accept("ENABLE_DUCKDB", String.valueOf(ENABLE_DUCKDB));
            variableWriter.apply(writer).accept("SL_VERSION", SL_VERSION);
            variableWriter.apply(writer).accept("SCALA_VERSION", SCALA_VERSION);
            variableWriter.apply(writer).accept("SPARK_VERSION", SPARK_VERSION);
            variableWriter.apply(writer).accept("HADOOP_VERSION", HADOOP_VERSION);
            variableWriter.apply(writer).accept("DUCKDB_VERSION", DUCKDB_VERSION);

            if (ENABLE_BIGQUERY || !anyDependencyEnabled()) {
                variableWriter.apply(writer).accept("SPARK_BQ_VERSION", SPARK_BQ_VERSION);
            }
            if (ENABLE_AZURE || !anyDependencyEnabled()) {
                variableWriter.apply(writer).accept("HADOOP_AZURE_VERSION", HADOOP_AZURE_VERSION);
                variableWriter.apply(writer).accept("AZURE_STORAGE_VERSION", AZURE_STORAGE_VERSION);
                variableWriter.apply(writer).accept("JETTY_VERSION", JETTY_VERSION);
            }
            if (ENABLE_SNOWFLAKE || !anyDependencyEnabled()) {
                variableWriter.apply(writer).accept("SPARK_SNOWFLAKE_VERSION", SPARK_SNOWFLAKE_VERSION);
                variableWriter.apply(writer).accept("SNOWFLAKE_JDBC_VERSION", SNOWFLAKE_JDBC_VERSION);
            }
            if (ENABLE_POSTGRESQL || !anyDependencyEnabled()) {
                variableWriter.apply(writer).accept("POSTGRESQL_VERSION", POSTGRESQL_VERSION);
            }
            if (ENABLE_TRINODB || !anyDependencyEnabled()) {
                variableWriter.apply(writer).accept("TRINODB_VERSION", TRINODB_VERSION);
            }
            if (ENABLE_REDSHIFT || !anyDependencyEnabled()) {
                variableWriter.apply(writer).accept("AWS_JAVA_SDK_VERSION", AWS_JAVA_SDK_VERSION);
                variableWriter.apply(writer).accept("HADOOP_AWS_VERSION", HADOOP_AWS_VERSION);
                variableWriter.apply(writer).accept("REDSHIFT_JDBC_VERSION", REDSHIFT_JDBC_VERSION);
                variableWriter.apply(writer).accept("SPARK_REDSHIFT_VERSION", SPARK_REDSHIFT_VERSION);
            }
            if (ENABLE_KAFKA || !anyDependencyEnabled()) {
                variableWriter.apply(writer).accept("CONFLUENT_VERSION", CONFLUENT_VERSION);
            }
            if (ENABLE_DUCKDB || !anyDependencyEnabled()) {
                variableWriter.apply(writer).accept("DUCKDB_VERSION", DUCKDB_VERSION);
            }
        } finally {
            writer.close();
        }
        System.out.println(versionFile.getAbsolutePath() + " created");
    }

    private static void generateVersions(File targetDir, boolean unix) throws IOException {
        if (isWindowsOs() && !unix) {
            generateWindowsVersions(targetDir);
        } else {
            generateUnixVersions(targetDir);
        }

    }

    private static boolean anyDependencyEnabled() {
        for (boolean enabled : getAllEnablers()) {
            if (enabled) {
                return true;
            }
        }
        return ENABLE_ALL;
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
    }

    @Override
    public List<Proxy> select(URI uri) {
        return Collections.singletonList(proxy);
    }

    @Override
    public void connectFailed(URI uri, SocketAddress sa, IOException ioe) {
        throw new RuntimeException("Failed to connect to " + uri + " using proxy " + sa);
    }

    private static final Setup instance = new Setup();

    private static final TrustManager alwaysTrustManager = instance;

    private static final ProxySelector proxySelector = instance;

    private static void setHttpClient() throws NoSuchAlgorithmException, KeyManagementException {
        setProxy();
        clientBuilder.proxy(proxySelector);
        if (username != null && password != null) {
            Authenticator authenticator = new UserPwdAuth();
            clientBuilder.authenticator(authenticator);
        }
        if (host != null && envIsTrue("SL_INSECURE")) {
            System.out.println("Enabling insecure mode for SSL connections using proxy " + protocol + "://" + host + ":" + port);
            // Create a trust manager that does not validate certificate chains
            TrustManager[] trustAllCerts = new TrustManager[]{alwaysTrustManager};

            // Install the all-trusting trust manager
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            clientBuilder.sslContext(sc);
        }
    }

    private static void updateSparkLog4j2Properties(File sparkDir) {
        File log4jFile = new File(new File(sparkDir, "conf"), "log4j2.properties");
        try {
            BufferedReader reader = new BufferedReader(new FileReader(log4jFile));
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("rootLogger.level =")|| line.startsWith("rootLogger.level=")) {
                    line = "rootLogger.level = ${env:SL_LOG_LEVEL:-error}";
                }
                sb.append(line).append("\n");
            }
            reader.close();

            sb.append("logger.shutdown.name=org.apache.spark.util.ShutdownHookManager").append("\n");
            sb.append("logger.shutdown.level=OFF").append("\n");
            sb.append("logger.env.name=org.apache.spark.SparkEnv").append("\n");
            sb.append("logger.env.level=error").append("\n");

            BufferedWriter writer = new BufferedWriter(new FileWriter(log4jFile));
            writer.write(sb.toString());
            writer.close();
        } catch (IOException e) {
            System.out.println("Failed to update log4j.properties");
            e.printStackTrace();
        }
    }

    private static void enableAllDependencies() {
        ENABLE_SPARK = true;
        ENABLE_AZURE = true;
        ENABLE_BIGQUERY = true;
        ENABLE_SNOWFLAKE = true;
        ENABLE_REDSHIFT = true;
        ENABLE_POSTGRESQL = true;
        ENABLE_MARIADB = true;
        ENABLE_CLICKHOUSE = false;
        ENABLE_DUCKDB = true;
        ENABLE_KAFKA = true;
        ENABLE_TRINODB = true;
    }

    private static void setTerminalRaw() throws IOException, InterruptedException {
        new ProcessBuilder("sh", "-c", "stty raw -echo < /dev/tty").inheritIO().start().waitFor();
    }

    private static void setTerminalCooked() throws IOException, InterruptedException {
        new ProcessBuilder("sh", "-c", "stty cooked echo < /dev/tty").inheritIO().start().waitFor();
    }

    private static void askUserWhichConfigToEnable() {
        if (isWindowsOs()) {
            askUserWhichConfigToEnableWindows();
        } else {
            askUserWhichConfigToEnableUnix();
        }
    }

    private static void askUserWhichConfigToEnableUnix() {
        try {
            setTerminalRaw();
            int currentSelection = 0;
            // Menu items mapping to flags
            // 0: Azure, 1: BigQuery, 2: Snowflake, 3: Redshift, 4: Postgres, 5: DuckDB, 6: Spark(noop), 7: Kafka, 8: Mariadb, 9: Trino, 10: All, 11: None, 12: Confirm
            boolean done = false;

            while (!done) {
                // Clear screen and cursor home
                System.out.print("\033[H\033[2J");
                
                System.out.print("Select dependencies (Use ARROW KEYS to move, TAB to toggle, ENTER to confirm):\r\n");
                System.out.print("\r\n");

                printMenuOption(0, currentSelection, ENABLE_AZURE, "Azure");
                printMenuOption(1, currentSelection, ENABLE_BIGQUERY, "BigQuery");
                printMenuOption(2, currentSelection, ENABLE_SNOWFLAKE, "Snowflake");
                printMenuOption(3, currentSelection, ENABLE_REDSHIFT, "Redshift");
                printMenuOption(4, currentSelection, ENABLE_POSTGRESQL, "Postgres");
                printMenuOption(5, currentSelection, ENABLE_DUCKDB, "DuckDB");
                printMenuOption(6, currentSelection, ENABLE_SPARK , "Spark");
                printMenuOption(7, currentSelection, ENABLE_KAFKA, "Kafka");
                printMenuOption(8, currentSelection, ENABLE_MARIADB, "Mariadb");
                printMenuOption(9, currentSelection, ENABLE_TRINODB, "Trino");
                
                System.out.print("\r\n");
                System.out.print((currentSelection == 10 ? " > " : "   ") + "[ Select All ]\r\n");
                System.out.print((currentSelection == 11 ? " > " : "   ") + "[ Select None ]\r\n");
                System.out.print((currentSelection == 12 ? " > " : "   ") + "[ DONE ]\r\n");

                // Read input
                int c = System.in.read();
                if (c == 27) { // ESC sequence
                    if (System.in.available() > 0) {
                        System.in.read(); // Skip [
                        int dir = System.in.read();
                        if (dir == 65) { // UP
                            currentSelection--;
                            if (currentSelection < 0) currentSelection = 12;
                        } else if (dir == 66) { // DOWN
                            currentSelection++;
                            if (currentSelection > 12) currentSelection = 0;
                        }
                    }
                } else if (c == 9 || c == 32) { // TAB or SPACE
                    toggleOption(currentSelection);
                } else if (c == 13 || c == 10) { // ENTER
                    if (currentSelection <= 9) {
                        toggleOption(currentSelection);
                    }
                    if (currentSelection == 10) { // All
                        enableAllDependencies();
                    } else if (currentSelection == 11) { // None
                        ENABLE_AZURE = false;
                        ENABLE_BIGQUERY = false;
                        ENABLE_SNOWFLAKE = false;
                        ENABLE_REDSHIFT = false;
                        ENABLE_POSTGRESQL = false;
                        ENABLE_DUCKDB = false;
                        ENABLE_KAFKA = false;
                        ENABLE_MARIADB = false;
                        ENABLE_TRINODB = false;
                        ENABLE_SPARK = false;
                    } else if (currentSelection == 12) { // Done
                        done = true;
                    }
                } else if (c == 3) { // Ctrl+C
                    setTerminalCooked();
                    System.exit(1);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                setTerminalCooked();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        
        if (!anyDependencyEnabled()) {
            System.out.println("No dependencies selected.");
            System.exit(1);
        } else {
            System.out.println("Installing selected dependencies...");
        }
    }

    private static void printMenuOption(int index, int currentSelection, boolean selected, String label) {
        String cursor = (index == currentSelection) ? " > " : "   ";
        String checkbox = selected ? "[x] " : "[ ] ";
        System.out.print(cursor + checkbox + label + "\r\n");
    }

    private static void toggleOption(int index) {
        switch (index) {
            case 0: ENABLE_AZURE = !ENABLE_AZURE; break;
            case 1: ENABLE_BIGQUERY = !ENABLE_BIGQUERY; break;
            case 2: ENABLE_SNOWFLAKE = !ENABLE_SNOWFLAKE; break;
            case 3: ENABLE_REDSHIFT = !ENABLE_REDSHIFT; break;
            case 4: ENABLE_POSTGRESQL = !ENABLE_POSTGRESQL; break;
            case 5: ENABLE_DUCKDB = !ENABLE_DUCKDB; break;
            case 6: ENABLE_SPARK = !ENABLE_SPARK; break;
            case 7: ENABLE_KAFKA = !ENABLE_KAFKA; break;
            case 8: ENABLE_MARIADB = !ENABLE_MARIADB; break;
            case 9: ENABLE_TRINODB = !ENABLE_TRINODB; break;
        }
    }
    private static void askUserWhichConfigToEnableWindows() {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            boolean done = false;
            while (!done) {
                // Clear screen
                System.out.print("\033[H\033[2J");
                System.out.flush();

                System.out.println("Please enable at least one of the following profiles to download the required dependencies:");
                System.out.println("Note: You may install more dependencies later by copying them to the bin/deps directory");
                System.out.println((ENABLE_AZURE ? "[x]" : "[ ]") + " Azure");
                System.out.println((ENABLE_BIGQUERY ? "[x]" : "[ ]") + " BigQuery");
                System.out.println((ENABLE_SNOWFLAKE ? "[x]" : "[ ]") + " Snowflake");
                System.out.println((ENABLE_REDSHIFT ? "[x]" : "[ ]") + " Redshift");
                System.out.println((ENABLE_POSTGRESQL ? "[x]" : "[ ]") + " Postgres");
                System.out.println((ENABLE_DUCKDB ? "[x]" : "[ ]") + " DuckDB");
                System.out.println((ENABLE_SPARK ? "[x]" : "[ ]") + " Spark");
                System.out.println((ENABLE_KAFKA ? "[x]" : "[ ]") + " Kafka");
                System.out.println((ENABLE_MARIADB ? "[x]" : "[ ]") + " Mariadb");
                System.out.println((ENABLE_TRINODB ? "[x]" : "[ ]") + " Trino");
                System.out.println("All");
                System.out.println("N) None");
                System.out.println("ENTER) Confirm selection");
                System.out.print("Enter choice to toggle (e.g. 1, 2...): ");

                String answer = reader.readLine();
                if (answer == null) break;
                answer = answer.trim();

                if (answer.isEmpty()) {
                    done = true;
                } else if (answer.equalsIgnoreCase("n")) {
                    ENABLE_AZURE = false;
                    ENABLE_BIGQUERY = false;
                    ENABLE_SNOWFLAKE = false;
                    ENABLE_REDSHIFT = false;
                    ENABLE_POSTGRESQL = false;
                    ENABLE_DUCKDB = false;
                    ENABLE_KAFKA = false;
                    ENABLE_MARIADB = false;
                    ENABLE_TRINODB = false;
                } else if (answer.equalsIgnoreCase("a")) {
                    enableAllDependencies();
                } else {
                    String[] choices = answer.split(",");
                    for (String choice : choices) {
                        switch (choice.trim()) {
                            case "1":
                                ENABLE_AZURE = !ENABLE_AZURE;
                                break;
                            case "2":
                                ENABLE_BIGQUERY = !ENABLE_BIGQUERY;
                                break;
                            case "3":
                                ENABLE_SNOWFLAKE = !ENABLE_SNOWFLAKE;
                                break;
                            case "4":
                                ENABLE_REDSHIFT = !ENABLE_REDSHIFT;
                                break;
                            case "5":
                                ENABLE_POSTGRESQL = !ENABLE_POSTGRESQL;
                                break;
                            case "6":
                                ENABLE_DUCKDB = !ENABLE_DUCKDB;
                                break;
                            case "7":
                                break;
                            case "8":
                                ENABLE_KAFKA = !ENABLE_KAFKA;
                                break;
                            case "9":
                                ENABLE_MARIADB = !ENABLE_MARIADB;
                                break;
                            case "10":
                                ENABLE_TRINODB = !ENABLE_TRINODB;
                                break;
                        }
                    }
                }
            }

            if (!anyDependencyEnabled()) {
                System.out.println("Please enable the configurations you want to use by setting the corresponding environment variables below");
                System.out.println("ENABLE_BIGQUERY, ENABLE_DATABRICKS, ENABLE_AZURE, ENABLE_SNOWFLAKE, ENABLE_DUCKDB, ENABLE_REDSHIFT, ENABLE_POSTGRESQL, ENABLE_ANY_JDBC, ENABLE_KAFKA, ENABLE_MARIADB, ENABLE_TRINODB");
                System.exit(1);
            } else {
                System.out.println("Installing selected dependencies...");
            }

        } catch (IOException e) {
            System.out.println("Failed to read user input");
            e.printStackTrace();
        }

    }
    public static void main(String[] args) throws IOException {
        try {
            if (args.length == 0) {
                System.out.println("Please specify the target directory");
                System.exit(1);
            }

            final File targetDir = new File(args[0]);
            if (!targetDir.exists()) {
                targetDir.mkdirs();
                System.out.println("Created target directory " + targetDir.getAbsolutePath());
            }

            setHttpClient();

            final File binDir = new File(targetDir, "bin");

            if (isWindowsOs()) {
                final File hadoopDir = new File(binDir, "hadoop");
                final File hadoopBinDir = new File(hadoopDir, "bin");
                deleteRecursively(hadoopDir);
                hadoopBinDir.mkdirs();
                for (ResourceDependency lib : HADOOP_LIBS) {
                    downloadAndDisplayProgress(lib, (resource, url) -> new File(hadoopBinDir, resource.getUrlName(url)));
                }

            }

            File slDir = new File(binDir, "sl");
            deleteRecursively(slDir);
            if (SL_VERSION.endsWith("SNAPSHOT")) {
                downloadAndDisplayProgress(new ResourceDependency[]{STARLAKE_SNAPSHOT_JAR}, slDir, false);
            } else {
                downloadAndDisplayProgress(new ResourceDependency[]{STARLAKE_RELEASE_JAR}, slDir, false);
            }

            if (ENABLE_API) {
                downloadApi(targetDir);
            }

            File sparkDir = new File(binDir, "spark");
            // deleteRecursively(sparkDir);
            if (!sparkDir.exists()) {
                downloadSpark(binDir);
            }


            File depsDir = new File(binDir, "deps");

            deleteDependencies(deltaSparkDependencies, depsDir);
            downloadAndDisplayProgress(deltaSparkDependencies, depsDir, true);

            deleteDependencies(icebergSparkDependencies, depsDir);
            downloadAndDisplayProgress(icebergSparkDependencies, depsDir, true);
            updateSparkLog4j2Properties(sparkDir);

            deleteDependencies(duckDbDependencies, depsDir);
            if (ENABLE_DUCKDB) {
                downloadAndDisplayProgress(duckDbDependencies, depsDir, true);
            }

            deleteDependencies(confluentDependencies, depsDir);
            if (ENABLE_KAFKA) {
                downloadAndDisplayProgress(confluentDependencies, depsDir, true);
            }

            deleteDependencies(redshiftDependencies, depsDir);
            if (ENABLE_REDSHIFT) {
                downloadAndDisplayProgress(redshiftDependencies, depsDir, true);
            }

            deleteDependencies(bigqueryDependencies, depsDir);
            if (ENABLE_BIGQUERY) {
                downloadAndDisplayProgress(bigqueryDependencies, depsDir, true);
            }

            deleteDependencies(azureDependencies, depsDir);
            if (ENABLE_AZURE) {
                downloadAndDisplayProgress(azureDependencies, depsDir, true);
            }

            deleteDependencies(snowflakeDependencies, depsDir);
            if (ENABLE_SNOWFLAKE) {
                downloadAndDisplayProgress(snowflakeDependencies, depsDir, true);
            }

            deleteDependencies(postgresqlDependencies, depsDir);
            if (ENABLE_POSTGRESQL) {
                downloadAndDisplayProgress(postgresqlDependencies, depsDir, true);
            }

            deleteDependencies(mariadbDependencies, depsDir);
            if (ENABLE_MARIADB) {
                downloadAndDisplayProgress(mariadbDependencies, depsDir, true);
            }

            deleteDependencies(clickhouseDependencies, depsDir);
            if (ENABLE_CLICKHOUSE) {
                downloadAndDisplayProgress(clickhouseDependencies, depsDir, true);
            }
            deleteDependencies(trinodbDependencies, depsDir);
            if (ENABLE_TRINODB) {
                downloadAndDisplayProgress(trinodbDependencies, depsDir, true);
            }

            downloadPythonLibs(new File(depsDir, "python-libs"));



            boolean unix = args.length > 1 && args[1].equalsIgnoreCase("unix");
            generateVersions(targetDir, unix);
        } catch (Exception e) {
            System.out.println("Failed to download dependency: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    public static void downloadApi(File targetDir) throws IOException, InterruptedException {
        final File projectsDir = new File(targetDir, "projects");
        final File demoDir = new File(projectsDir, "demo");
        final File binDir = new File(targetDir, "bin");
        File apiDir = new File(binDir, "api");
        deleteRecursively(apiDir);


        ResourceDependency apiZip = SL_API_VERSION.endsWith("SNAPSHOT") ? SL_API_SNAPSHOT_ZIP : SL_API_RELEASE_ZIP;
        downloadAndDisplayProgress(new ResourceDependency[]{apiZip}, binDir, false);
        apiZip.getUrlNames().stream().map(zipName -> new File(binDir, zipName)).filter(File::exists).forEach(zipFile -> {
            try {
                unzip(zipFile, binDir);
            } catch (IOException e) {
                System.out.println("Failed to extract API: " + e.getMessage());
                e.printStackTrace();
            }
            zipFile.delete();

            // Rename extracted directory
            File extractedDir = new File(binDir, apiZip.artefactName + "-" + SL_API_VERSION);
            File renamedDir = new File(binDir, "api");

            if (extractedDir.exists()) {
                System.out.println("Renaming " + extractedDir.getAbsolutePath() + " to " + renamedDir.getAbsolutePath());
                extractedDir.renameTo(renamedDir);
            }
        });
        File starbakeZip = new File(apiDir, "starbake.zip");
        final File demoStarbakeZip = new File(demoDir, "starbake.zip");
        demoDir.mkdirs();
        if (starbakeZip.exists() && !starbakeZip.renameTo(demoStarbakeZip)) {
            System.out.println("Failed to rename " + starbakeZip.getAbsolutePath() + " to " + demoStarbakeZip.getAbsolutePath());
        }
        File tpch001Zip = new File(apiDir, "tpch001.zip");
        final File demoTpch001Zip = new File(demoDir, "tpch001.zip");
        demoDir.mkdirs();
        if (starbakeZip.exists() && !starbakeZip.renameTo(demoTpch001Zip)) {
            System.out.println("Failed to rename " + starbakeZip.getAbsolutePath() + " to " + demoTpch001Zip.getAbsolutePath());
        }
    }

    public static void downloadSpark(File binDir) throws IOException, InterruptedException {
        ResourceDependency sparkJar = SPARK_JAR;
        downloadAndDisplayProgress(new ResourceDependency[]{sparkJar}, binDir, false);
        sparkJar.getUrlNames().stream().map(tgzName -> new File(binDir, tgzName)).filter(File::exists).forEach(sparkFile -> {
            String tgzName = sparkFile.getName();
            ProcessBuilder builder = new ProcessBuilder("tar", "-xzf", sparkFile.getAbsolutePath(), "-C", binDir.getAbsolutePath()).inheritIO();
            try {
                Process process = builder.start();
                process.waitFor();
            } catch (InterruptedException | IOException e) {
                System.out.println("Failed to extract spark tarball");
                e.printStackTrace();
            }
            sparkFile.delete();
            File sparkDir = new File(binDir, tgzName.substring(0, tgzName.lastIndexOf(".")));
            sparkDir.renameTo(new File(binDir, "spark"));
            sparkDir = new File(binDir, "spark");
            File log4j2File = new File(sparkDir, "conf/log4j2.properties.template");
            log4j2File.renameTo(new File(sparkDir, "conf/log4j2.properties"));
        });
    }

    private static void downloadPythonLibs(File targetDir) throws IOException, InterruptedException {
        if (!targetDir.exists()) {
            targetDir.mkdirs();
        }
        ResourceDependency versionsFile = new ResourceDependency("versions.txt", PYTHON_LIBS_URL + "versions.txt");
        downloadAndDisplayProgress(versionsFile, (resource, url) -> new File(targetDir, "versions.txt"));
        
        File versionsTxt = new File(targetDir, "versions.txt");
        if (versionsTxt.exists()) {
            List<String> filesToDownload = new ArrayList<>();
           try (BufferedReader br = new BufferedReader(new FileReader(versionsTxt))) {
               String line;
               while ((line = br.readLine()) != null) {
                   String trimmedLine = line.trim();
                   if (!trimmedLine.isEmpty() && !trimmedLine.startsWith("#")) {
                       filesToDownload.add(trimmedLine);
                   }
               }
           }
           if (!filesToDownload.isEmpty()) {
               ResourceDependency[] dependencies = filesToDownload.stream().map(fileName -> 
                   new ResourceDependency(fileName, PYTHON_LIBS_URL + fileName)
               ).toArray(ResourceDependency[]::new);
               downloadAndDisplayProgress(dependencies, targetDir, true);
           }
        }
    }

    private static void downloadAndDisplayProgress(ResourceDependency[] dependencies, File targetDir, boolean replaceJar) throws IOException, InterruptedException {
        if (!targetDir.exists()) {
            targetDir.mkdirs();
        }
        if (replaceJar) {
            deleteDependencies(dependencies, targetDir);
        }
        for (ResourceDependency dependency : dependencies) {
            downloadAndDisplayProgress(dependency, (resource, url) -> new File(targetDir, resource.getUrlName(url)));
        }
    }

    private static void deleteDependencies(ResourceDependency[] dependencies, File targetDir) {
        if (targetDir.exists()) {
            for (ResourceDependency dependency : dependencies) {
                File[] files = targetDir.listFiles(f -> f.getPath().contains(dependency.artefactName));
                if (files != null) {
                    for (File file : files) {
                        deleteFile(file);
                    }
                }
            }
        }
    }

    private static void deleteRecursively(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteRecursively(file);
            }
        }
        directoryToBeDeleted.delete();
    }

    private static void deleteFile(File file) {
        if (file.exists()) {
            if (file.delete()) {
                System.out.println(file.getAbsolutePath() + " deleted");
            }
        }
    }

    private static void downloadAndDisplayProgress(ResourceDependency resource, BiFunction<ResourceDependency, String, File> fileProducer) throws IOException, InterruptedException {
        try {
            client = clientBuilder.followRedirects(HttpClient.Redirect.ALWAYS).build();
            boolean succesfullyDownloaded = false;
            List<String> triedUrlList = new ArrayList<>();
            System.out.println("Downloading " + resource.artefactName + "...");
            for (String urlStr : resource.urls) {
                System.out.println("from " + urlStr);
                File file = fileProducer.apply(resource, urlStr);
                final int CHUNK_SIZE = 1024;
                int filePartIndex = urlStr.lastIndexOf("/") + 1;
                String urlFolder = urlStr.substring(0, filePartIndex);
                int qIdx = urlFolder.indexOf("?");
                if (qIdx >= 0) urlFolder = urlFolder.substring(0, qIdx);
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(urlStr))
                        .build();
                HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
                if (response.statusCode() == 200) {
                    long lengthOfFile = response.headers().firstValueAsLong("Content-Length").orElse(0L);
                    try (InputStream input = new BufferedInputStream(response.body());
                         OutputStream output = new FileOutputStream(file)) {
                        byte[] data = new byte[CHUNK_SIZE];
                        long total = 0;
                        int count;
                        int loop = 0;
                        int sbLen = 0;
                        long lastTime = System.currentTimeMillis();
                        while ((count = input.read(data)) != -1) {
                            total += count;
                            output.write(data, 0, count);
                            loop++;
                            if (loop % 1000 == 0) {
                                StringBuilder sb = new StringBuilder(" " + (total / 1024 / 1024) + "/" + (lengthOfFile / 1024 / 1024) + " MB");
                                if (lengthOfFile > 0) {
                                    sb.append(" (");
                                    sb.append(total * 100 / lengthOfFile);
                                    sb.append("%)");
                                }
                                long currentTime = System.currentTimeMillis();
                                long timeDiff = currentTime - lastTime;
                                double bytesPerMilliSec = (CHUNK_SIZE * 1000.0 / timeDiff);
                                double bytesPerSec = bytesPerMilliSec * 1000;
                                double mbPerSec = bytesPerSec / 1024 / 1024;
                                sb.append(" ");
                                sb.append(String.format("[%.2f MB/sec]", mbPerSec));
                                lastTime = currentTime;
                                sbLen = sb.length();
                                for (int cnt = 0; cnt < sbLen; cnt++) {
                                    System.out.print("\b");
                                }
                                System.out.print(sb);
                            }
                        }
                        for (int cnt = 0; cnt < sbLen; cnt++) {
                            System.out.print("\b");
                        }
                        System.out.print(file.getAbsolutePath() + " succesfully downloaded from " + urlFolder);
                        System.out.println();
                    }
                    succesfullyDownloaded = true;
                    break;
                } else {
                    triedUrlList.add(urlStr + " (" + response.statusCode() + ")");
                }
            }
            if (!succesfullyDownloaded) {
                String triedUrls = String.join(" and ", triedUrlList);
                throw new RuntimeException("Failed to fetch " + resource.artefactName + " from " + triedUrls);
            }
        }  catch (IOException | InterruptedException e) {
            System.out.println("Failed to download " + resource.artefactName + " from " + resource.urls);
            throw e;
        }
        finally {
            client = null; // Close the client to release resources
        }
    }

    // Utility method to unzip using Java
    private static void unzip(File zipFile, File destDir) throws IOException {
        try (ZipInputStream zipIn = new ZipInputStream(new FileInputStream(zipFile))) {
            ZipEntry entry = zipIn.getNextEntry();
            while (entry != null) {
                File filePath = new File(destDir, entry.getName());
                if (entry.isDirectory()) {
                    filePath.mkdirs();
                } else {
                    // Create parent dirs if they don't exist
                    filePath.getParentFile().mkdirs();
                    try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath))) {
                        byte[] buffer = new byte[4096];
                        int bytesRead;
                        while ((bytesRead = zipIn.read(buffer)) != -1) {
                            bos.write(buffer, 0, bytesRead);
                        }
                    }
                }
                zipIn.closeEntry();
                entry = zipIn.getNextEntry();
            }
        }
    }
}
