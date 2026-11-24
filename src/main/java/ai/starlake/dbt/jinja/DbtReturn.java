package ai.starlake.dbt.jinja;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * dbt's {@code return(value)}: the macro call being evaluated returns {@code value} instead of its
 * text. Each call of a {@link ReturningMacro} opens a slot; {@code return} fills the innermost one,
 * once. As in dbt, where {@code return} raises, what the macro would render after it has no effect:
 * the macro stops at its next top-level node, and the effects of the dbt objects check {@link
 * #anyReturned()} for what is still evaluated inside the node that returned.
 */
public final class DbtReturn {
  private static final ThreadLocal<Deque<Object[]>> SLOTS =
      ThreadLocal.withInitial(ArrayDeque::new);

  private DbtReturn() {}

  /** The Jinja function {@code return}: renders nothing. */
  public static Object ret(Object value) {
    Object[] slot = SLOTS.get().peek();
    if (slot != null && slot[0] == null) slot[0] = new Object[] {value};
    return "";
  }

  static Object[] enter() {
    Object[] slot = new Object[1];
    SLOTS.get().push(slot);
    return slot;
  }

  static void exit() {
    SLOTS.get().pop();
  }

  static boolean returned(Object[] slot) {
    return slot[0] != null;
  }

  /** The innermost macro call has returned: the rest of its body is not rendered */
  static boolean innermostReturned() {
    Object[] slot = SLOTS.get().peek();
    return slot != null && slot[0] != null;
  }

  /** A macro call being evaluated has returned: what is rendered now has no effect */
  public static boolean anyReturned() {
    for (Object[] slot : SLOTS.get()) if (slot[0] != null) return true;
    return false;
  }

  static Object value(Object[] slot) {
    return ((Object[]) slot[0])[0];
  }
}
