package xyz.triplox.client;

import java.util.List;

/** One z-set row of a {@link Delta}: positional values plus a signed weight. */
public record Row(List<Object> values, long weight) {}
