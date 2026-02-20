package de.luh.vss.chat.dtq.metrics;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Minimal plotting helper using only AWT (no external libs).
 * Generates PNG charts for speedup and sync/async comparisons.
 */
public final class PlotGenerator {

    private PlotGenerator() {}

    /**
     * Generates a speedup chart from {@code logs/speedup.csv}.
     */
    public static Path generateSpeedupChart(Path outPath) {
        List<SpeedupPoint> points = readSpeedupCsv(Paths.get("logs/speedup.csv"));
        if (points.isEmpty()) {
            System.err.println("No speedup data found in logs/speedup.csv");
            return null;
        }
        Map<String, List<SpeedupPoint>> byMode = points.stream()
                .collect(Collectors.groupingBy(p -> p.mode));
        List<Series> seriesList = new ArrayList<>();
        for (Map.Entry<String, List<SpeedupPoint>> entry : byMode.entrySet()) {
            String mode = entry.getKey();
            List<SpeedupPoint> sorted = entry.getValue().stream()
                    .sorted(Comparator.comparingInt(p -> p.workers))
                    .toList();
            double baseline = sorted.stream()
                    .filter(p -> p.workers == 1)
                    .mapToDouble(p -> p.wallMs)
                    .findFirst()
                    .orElse(sorted.get(0).wallMs);
            List<Point> pts = sorted.stream()
                    .map(p -> new Point(p.workers, baseline / p.wallMs))
                    .toList();
            seriesList.add(new Series(mode, pts, pickColor(seriesList.size())));
        }
        return drawLineChart("Speedup vs workers", "Workers", "Speedup", seriesList, outPath);
    }

    /**
     * Generates a sync vs async wall time comparison chart from speedup log.
     */
    public static Path generateSyncVsAsyncChart(Path outPath) {
        List<SpeedupPoint> points = readSpeedupCsv(Paths.get("logs/speedup.csv"));
        if (points.isEmpty()) {
            System.err.println("No speedup data found in logs/speedup.csv");
            return null;
        }
        Map<Integer, Map<String, Double>> grouped = points.stream()
                .collect(Collectors.groupingBy(p -> p.workers,
                        Collectors.groupingBy(p -> p.mode,
                                Collectors.averagingDouble(p -> (double) p.wallMs))));

        List<Series> seriesList = new ArrayList<>();
        seriesList.add(buildBarSeries("sync", grouped, pickColor(0)));
        seriesList.add(buildBarSeries("async", grouped, pickColor(1)));
        return drawBarChart("Wall time: sync vs async", "Workers", "Wall time (ms)",
                seriesList, outPath);
    }



    private record SpeedupPoint(int workers, String mode, long wallMs) {}
    private record Point(double x, double y) {}
    private record Series(String name, List<Point> points, Color color) {}

    private static List<SpeedupPoint> readSpeedupCsv(Path path) {
        if (path == null || !Files.exists(path)) {
            return List.of();
        }
        try {
            return Files.readAllLines(path, StandardCharsets.UTF_8).stream()
                    .skip(1) // header
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .map(PlotGenerator::parseSpeedupRow)
                    .filter(p -> p != null)
                    .toList();
        } catch (IOException e) {
            System.err.println("Failed to read speedup.csv: " + e.getMessage());
            return List.of();
        }
    }

    private static SpeedupPoint parseSpeedupRow(String row) {
        String[] parts = row.split(",", -1);
        if (parts.length < 5) return null;
        try {
            int workers = Integer.parseInt(parts[2].trim());
            String mode = parts[3].trim();
            long wall = Long.parseLong(parts[4].trim());
            return new SpeedupPoint(workers, mode, wall);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Color pickColor(int idx) {
        Color[] palette = new Color[]{
                new Color(45, 156, 219),
                new Color(111, 207, 151),
                new Color(252, 132, 124),
                new Color(255, 204, 112),
                new Color(171, 148, 255)
        };
        return palette[idx % palette.length];
    }

    private static Series buildBarSeries(String mode,
                                         Map<Integer, Map<String, Double>> grouped,
                                         Color color) {
        List<Point> pts = grouped.entrySet().stream()
                .map(e -> new Point(e.getKey(), e.getValue().getOrDefault(mode, 0.0)))
                .sorted(Comparator.comparingDouble(Point::x))
                .toList();
        return new Series(mode, pts, color);
    }

    private static Path drawLineChart(String title,
                                      String xLabel,
                                      String yLabel,
                                      List<Series> seriesList,
                                      Path outPath) {
        int width = 900;
        int height = 520;
        int margin = 80;
        BufferedImage img = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g = img.createGraphics();
        setupG(g);
        g.setColor(Color.WHITE);
        g.fillRect(0, 0, width, height);

        double maxX = seriesList.stream()
                .flatMap(s -> s.points.stream())
                .mapToDouble(p -> p.x)
                .max().orElse(1.0);
        double maxY = seriesList.stream()
                .flatMap(s -> s.points.stream())
                .mapToDouble(p -> p.y)
                .max().orElse(1.0);
        if (maxY < 1.0) maxY = 1.0;

        // axes
        g.setColor(Color.DARK_GRAY);
        g.setStroke(new BasicStroke(1.5f));
        int x0 = margin;
        int y0 = height - margin;
        int x1 = width - margin;
        int y1 = margin;
        g.drawLine(x0, y0, x1, y0);
        g.drawLine(x0, y0, x0, y1);

        // labels
        drawCentered(g, title, width / 2, 30, new Font("SansSerif", Font.BOLD, 18));
        drawCentered(g, xLabel, width / 2, height - 20, new Font("SansSerif", Font.PLAIN, 14));
        drawRotatedLeft(g, yLabel, 20, height / 2, new Font("SansSerif", Font.PLAIN, 14));

        // grid + ticks
        g.setColor(new Color(200, 200, 200));
        int ticks = 4;
        for (int i = 1; i <= ticks; i++) {
            int y = y0 - (i * (y0 - y1) / ticks);
            g.drawLine(x0, y, x1, y);
            double val = (maxY / ticks) * i;
            g.setColor(Color.DARK_GRAY);
            g.drawString(String.format(Locale.US, "%.1f", val), x0 - 45, y + 5);
            g.setColor(new Color(200, 200, 200));
        }

        // draw series
        for (Series s : seriesList) {
            g.setColor(s.color);
            g.setStroke(new BasicStroke(2.2f));
            Point prev = null;
            for (Point p : s.points) {
                int x = x0 + (int) ((p.x / maxX) * (x1 - x0));
                int y = y0 - (int) ((p.y / maxY) * (y0 - y1));
                g.fillOval(x - 4, y - 4, 8, 8);
                if (prev != null) {
                    g.drawLine(x0 + (int) ((prev.x / maxX) * (x1 - x0)),
                            y0 - (int) ((prev.y / maxY) * (y0 - y1)),
                            x, y);
                }
                prev = p;
            }
        }

        // legend
        int legendX = x1 - 140;
        int legendY = y1 + 20;
        g.setFont(new Font("SansSerif", Font.PLAIN, 13));
        for (int i = 0; i < seriesList.size(); i++) {
            Series s = seriesList.get(i);
            g.setColor(s.color);
            g.fillRect(legendX, legendY + i * 20 - 10, 16, 10);
            g.setColor(Color.DARK_GRAY);
            g.drawString(s.name, legendX + 22, legendY + i * 20);
        }

        g.dispose();
        return writeImage(img, outPath != null ? outPath : Paths.get("logs/speedup.png"));
    }

    private static Path drawBarChart(String title,
                                     String xLabel,
                                     String yLabel,
                                     List<Series> seriesList,
                                     Path outPath) {
        int width = 900;
        int height = 520;
        int margin = 80;
        BufferedImage img = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g = img.createGraphics();
        setupG(g);
        g.setColor(Color.WHITE);
        g.fillRect(0, 0, width, height);

        double maxX = seriesList.stream()
                .flatMap(s -> s.points.stream())
                .mapToDouble(Point::x)
                .max().orElse(1.0);
        double maxY = seriesList.stream()
                .flatMap(s -> s.points.stream())
                .mapToDouble(Point::y)
                .max().orElse(1.0);
        if (maxY < 1.0) maxY = 1.0;

        int x0 = margin;
        int y0 = height - margin;
        int x1 = width - margin;
        int y1 = margin;

        g.setColor(Color.DARK_GRAY);
        g.setStroke(new BasicStroke(1.5f));
        g.drawLine(x0, y0, x1, y0);
        g.drawLine(x0, y0, x0, y1);

        drawCentered(g, title, width / 2, 30, new Font("SansSerif", Font.BOLD, 18));
        drawCentered(g, xLabel, width / 2, height - 20, new Font("SansSerif", Font.PLAIN, 14));
        drawRotatedLeft(g, yLabel, 20, height / 2, new Font("SansSerif", Font.PLAIN, 14));

        // bars grouped by worker count
        double groupWidth = (x1 - x0) / Math.max(1, maxX);
        double barWidth = groupWidth / (seriesList.size() + 1);

        for (int i = 0; i < seriesList.size(); i++) {
            Series s = seriesList.get(i);
            g.setColor(s.color);
            for (Point p : s.points) {
                double gx = x0 + (p.x - 1) * groupWidth;
                int barX = (int) (gx + i * barWidth + barWidth * 0.15);
                int barH = (int) ((p.y / maxY) * (y0 - y1));
                int barY = y0 - barH;
                g.fillRect(barX, barY, (int) (barWidth * 0.7), barH);
            }
        }

        // legend
        int legendX = x1 - 160;
        int legendY = y1 + 20;
        g.setFont(new Font("SansSerif", Font.PLAIN, 13));
        for (int i = 0; i < seriesList.size(); i++) {
            Series s = seriesList.get(i);
            g.setColor(s.color);
            g.fillRect(legendX, legendY + i * 20 - 10, 16, 10);
            g.setColor(Color.DARK_GRAY);
            g.drawString(s.name, legendX + 22, legendY + i * 20);
        }

        g.dispose();
        return writeImage(img, outPath != null ? outPath : Paths.get("logs/sync_async.png"));
    }

    private static void setupG(Graphics2D g) {
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
    }

    private static void drawCentered(Graphics2D g, String text, int x, int y, Font font) {
        Font old = g.getFont();
        g.setFont(font);
        FontMetrics fm = g.getFontMetrics();
        int w = fm.stringWidth(text);
        g.setColor(Color.DARK_GRAY);
        g.drawString(text, x - w / 2, y);
        g.setFont(old);
    }

    private static void drawRotatedLeft(Graphics2D g, String text, int x, int y, Font font) {
        Font old = g.getFont();
        g.setFont(font);
        g.rotate(-Math.PI / 2, x, y);
        g.setColor(Color.DARK_GRAY);
        g.drawString(text, x, y);
        g.rotate(Math.PI / 2, x, y);
        g.setFont(old);
    }

    private static Path writeImage(BufferedImage img, Path outPath) {
        try {
            Files.createDirectories(outPath.getParent());
            javax.imageio.ImageIO.write(img, "png", outPath.toFile());
            System.out.println("Saved plot to " + outPath.toAbsolutePath());
            return outPath;
        } catch (IOException e) {
            System.err.println("Failed to write plot: " + e.getMessage());
            return null;
        }
    }
}
