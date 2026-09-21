package ai.pipestream.quic.storage;

import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

/**
 * Immutable logical address of one scope. A scope is a path of 1 to 8 unsigned
 * 32-bit components: the root scope is a single component at depth 0 and each
 * child scope adds one level, so protocol nesting depth is bounded to 0-7.
 * The rendered form is one lowercase, zero-padded, 8-hex-digit component per
 * level; this rendered form is the shared cross-language and cross-backend name.
 */
public record ScopePath(List<Long> components) {
  /** Number of levels below the root scope; the protocol bound is 0-7. */
  public static final int MAX_DEPTH = 7;
  /** Largest unsigned 32-bit component value. */
  public static final long MAX_COMPONENT = 0xffff_ffffL;

  /**
   * Copies and validates the component list.
   * @param components 1 to 8 unsigned 32-bit levels, outermost first
   */
  public ScopePath {
    components = List.copyOf(components);
    if (components.isEmpty() || components.size() > MAX_DEPTH + 1) {
      throw new IllegalArgumentException("scope path needs 1 to " + (MAX_DEPTH + 1) + " components");
    }
    for (long component : components) {
      if (component < 0 || component > MAX_COMPONENT) {
        throw new IllegalArgumentException("scope component is not an unsigned 32-bit value");
      }
    }
  }

  /**
   * Creates a path from individual components.
   * @param components 1 to 8 unsigned 32-bit levels, outermost first
   * @return validated scope path
   */
  public static ScopePath of(long... components) {
    Long[] boxed = new Long[components.length];
    for (int i = 0; i < components.length; i++) boxed[i] = components[i];
    return new ScopePath(List.of(boxed));
  }

  /**
   * Creates the root scope for one top-level component.
   * @param component unsigned 32-bit root scope
   * @return single-component scope path at depth 0
   */
  public static ScopePath root(long component) {
    return new ScopePath(List.of(component));
  }

  /**
   * Parses the canonical {@link #render()}ed form: one to eight components
   * of exactly eight lowercase hex digits joined by '/'.
   * @param rendered canonical scope path string
   * @return validated scope path
   * @throws IllegalArgumentException for null, empty, or non-canonical input
   */
  public static ScopePath parse(String rendered) {
    Objects.requireNonNull(rendered, "rendered");
    if (rendered.isEmpty()) throw new IllegalArgumentException("empty scope path");
    String[] levels = rendered.split("/", -1);
    long[] components = new long[levels.length];
    for (int i = 0; i < levels.length; i++) {
      if (!levels[i].matches("[0-9a-f]{8}")) {
        throw new IllegalArgumentException("scope path level is not 8 lowercase hex digits: " + levels[i]);
      }
      components[i] = Long.parseLong(levels[i], 16);
    }
    return of(components);
  }

  /** @return nesting depth, 0 for a root scope, at most {@link #MAX_DEPTH} */
  public int depth() {
    return components.size() - 1;
  }

  /**
   * Renders the path as lowercase zero-padded 8-hex-digit components joined by '/'.
   * @return canonical cross-backend scope name
   */
  public String render() {
    StringBuilder name = new StringBuilder(components.size() * 9 - 1);
    for (int i = 0; i < components.size(); i++) {
      if (i > 0) name.append('/');
      name.append(HexFormat.of().toHexDigits(components.get(i), 8));
    }
    return name.toString();
  }

  @Override public String toString() {
    return render();
  }
}
