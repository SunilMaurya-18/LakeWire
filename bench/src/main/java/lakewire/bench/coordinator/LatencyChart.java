package lakewire.bench.coordinator;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import lakewire.bench.coordinator.CoordinatorLatencyLab.Sample;
import lakewire.bench.coordinator.CoordinatorLatencyLab.Strategy;

public final class LatencyChart {

    private static final int[] PARTITIONS = {1, 16, 128, 512, 1024};
    private static final int[] COMMITTERS = {1, 3, 6, 12};

    private LatencyChart() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("usage: LatencyChart <results.csv> <chart.svg>");
        }
        write(Path.of(args[1]), read(Path.of(args[0])));
    }

    private static List<Sample> read(Path csv) throws Exception {
        List<Sample> samples = new ArrayList<>();
        for (String line : Files.readAllLines(csv)) {
            if (line.isBlank() || line.startsWith("#") || line.startsWith("strategy,")) {
                continue;
            }
            String[] columns = line.split(",");
            Strategy strategy = switch (columns[0]) {
                case "row-loop" -> Strategy.ROW_LOOP;
                case "set-based" -> Strategy.SET_BASED;
                case "group-commit" -> Strategy.GROUP;
                default -> throw new IllegalArgumentException(columns[0]);
            };
            samples.add(new Sample(
                strategy,
                Integer.parseInt(columns[1]),
                Integer.parseInt(columns[2]),
                Integer.parseInt(columns[3]),
                Double.parseDouble(columns[4]),
                Double.parseDouble(columns[5]),
                Double.parseDouble(columns[6])
            ));
        }
        return samples;
    }

    static void write(Path path, List<Sample> samples) throws Exception {
        StringBuilder svg = new StringBuilder();
        svg.append("""
            <svg xmlns="http://www.w3.org/2000/svg" width="960" height="640" viewBox="0 0 960 640">
            <rect width="960" height="640" fill="#fafaf9"/>
            <text x="32" y="32" font-family="sans-serif" font-size="20" fill="#1c1917">Commit P99 by partitions per commit</text>
            <text x="32" y="54" font-family="sans-serif" font-size="12" fill="#57534e">Testcontainers postgres:18.7-alpine, fsync=off. Each panel has its own P99 scale, in milliseconds.</text>
            """);
        legend(svg, 32, 72);
        int panelWidth = 430;
        int panelHeight = 240;
        int originX = 40;
        int originY = 100;
        for (int i = 0; i < COMMITTERS.length; i++) {
            int column = i % 2;
            int row = i / 2;
            int x = originX + column * (panelWidth + 20);
            int y = originY + row * (panelHeight + 16);
            panel(svg, samples, COMMITTERS[i], x, y, panelWidth, panelHeight);
        }
        svg.append("</svg>\n");
        Files.writeString(path, svg.toString());
    }

    private static void legend(StringBuilder svg, int x, int y) {
        String[] labels = {"row-loop", "set-based", "group-commit"};
        String[] colors = {"#c2410c", "#1d4ed8", "#047857"};
        for (int i = 0; i < labels.length; i++) {
            int left = x + i * 150;
            svg.append("<line x1=\"").append(left).append("\" y1=\"").append(y)
                .append("\" x2=\"").append(left + 28).append("\" y2=\"").append(y)
                .append("\" stroke=\"").append(colors[i]).append("\" stroke-width=\"2\"/>");
            svg.append("<text x=\"").append(left + 34).append("\" y=\"").append(y + 4)
                .append("\" font-family=\"sans-serif\" font-size=\"12\" fill=\"#1c1917\">")
                .append(labels[i]).append("</text>");
        }
    }

    private static void panel(
        StringBuilder svg,
        List<Sample> samples,
        int committers,
        int x,
        int y,
        int width,
        int height
    ) {
        double panelMax = 1;
        for (Sample sample : samples) {
            if (sample.committers() == committers) {
                panelMax = Math.max(panelMax, sample.p99Ms());
            }
        }
        double yMax = panelMax * 1.15;
        int left = x + 48;
        int top = y + 28;
        int plotWidth = width - 64;
        int plotHeight = height - 64;
        svg.append("<rect x=\"").append(x).append("\" y=\"").append(y)
            .append("\" width=\"").append(width).append("\" height=\"").append(height)
            .append("\" rx=\"8\" fill=\"#ffffff\" stroke=\"#e7e5e4\"/>");
        svg.append("<text x=\"").append(left).append("\" y=\"").append(y + 18)
            .append("\" font-family=\"sans-serif\" font-size=\"13\" fill=\"#1c1917\">")
            .append(committers).append(committers == 1 ? " committer" : " committers").append("</text>");

        for (int tick = 0; tick <= 4; tick++) {
            int yLine = top + plotHeight - (plotHeight * tick / 4);
            double value = yMax * tick / 4.0;
            svg.append("<line x1=\"").append(left).append("\" y1=\"").append(yLine)
                .append("\" x2=\"").append(left + plotWidth).append("\" y2=\"").append(yLine)
                .append("\" stroke=\"#e7e5e4\"/>");
            svg.append("<text x=\"").append(left - 6).append("\" y=\"").append(yLine + 4)
                .append("\" text-anchor=\"end\" font-family=\"sans-serif\" font-size=\"10\" fill=\"#78716c\">")
                .append(String.format(Locale.ROOT, "%.0f", value)).append("</text>");
        }
        for (int i = 0; i < PARTITIONS.length; i++) {
            int xTick = left + xAt(i, plotWidth);
            svg.append("<text x=\"").append(xTick).append("\" y=\"").append(top + plotHeight + 16)
                .append("\" text-anchor=\"middle\" font-family=\"sans-serif\" font-size=\"10\" fill=\"#78716c\">")
                .append(PARTITIONS[i]).append("</text>");
        }

        line(svg, samples, committers, Strategy.ROW_LOOP, "#c2410c", left, top, plotWidth, plotHeight, yMax);
        line(svg, samples, committers, Strategy.SET_BASED, "#1d4ed8", left, top, plotWidth, plotHeight, yMax);
        line(svg, samples, committers, Strategy.GROUP, "#047857", left, top, plotWidth, plotHeight, yMax);
    }

    private static void line(
        StringBuilder svg,
        List<Sample> samples,
        int committers,
        Strategy strategy,
        String color,
        int left,
        int top,
        int plotWidth,
        int plotHeight,
        double yMax
    ) {
        List<String> points = new ArrayList<>();
        for (int i = 0; i < PARTITIONS.length; i++) {
            Sample sample = find(samples, strategy, PARTITIONS[i], committers);
            if (sample == null) {
                continue;
            }
            int x = left + xAt(i, plotWidth);
            int y = top + plotHeight - (int) Math.round(plotHeight * (sample.p99Ms() / yMax));
            points.add(x + "," + y);
        }
        if (points.isEmpty()) {
            return;
        }
        svg.append("<polyline fill=\"none\" stroke=\"").append(color)
            .append("\" stroke-width=\"2\" points=\"").append(String.join(" ", points)).append("\"/>");
    }

    private static Sample find(List<Sample> samples, Strategy strategy, int partitions, int committers) {
        for (Sample sample : samples) {
            if (sample.strategy() == strategy && sample.partitions() == partitions && sample.committers() == committers) {
                return sample;
            }
        }
        return null;
    }

    private static int xAt(int index, int plotWidth) {
        if (PARTITIONS.length == 1) {
            return plotWidth / 2;
        }
        return plotWidth * index / (PARTITIONS.length - 1);
    }
}
