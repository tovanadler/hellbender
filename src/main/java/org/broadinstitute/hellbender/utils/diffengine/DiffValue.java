package org.broadinstitute.hellbender.utils.diffengine;

/**
 * An interface that must be implemented to allow us to calculate differences
 * between structured objects
 */
public class DiffValue {
    private DiffElement binding = null;
    final private Object value;

    public DiffValue(Object value) {
        this.value = value;
    }

    public DiffValue(DiffElement binding, Object value) {
        this.binding = binding;
        this.value = value;
    }

    public DiffValue(DiffValue parent, Object value) {
        this(parent.getBinding(), value);
    }

    public DiffValue(String name, DiffElement parent, Object value) {
        this.binding = new DiffElement(name, parent, this);
        this.value = value;
    }

    public DiffValue(String name, DiffValue parent, Object value) {
        this(name, parent.getBinding(), value);
    }

    public DiffElement getBinding() {
        return binding;
    }

    protected void setBinding(DiffElement binding) {
        this.binding = binding;
    }

    public Object getValue() {
        return value;
    }

    public String toString() {
        return getValue().toString();
    }

    public String toString(int offset) {
        return toString();
    }

    public String toOneLineString() {
        return getValue().toString();
    }

    public boolean isAtomic() { return true; }

    public boolean isCompound() { return ! isAtomic(); }

    public int size() { return 1; }
}
