package org.broadinstitute.hellbender.utils.diffengine;

import com.google.common.base.Strings;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.utils.Utils;

import java.util.*;

/**
 * An interface that must be implemented to allow us to calculate differences
 * between structured objects
 */
public final class DiffNode extends DiffValue {
    private Map<String, DiffElement> getElementMap() {
        return (Map<String, DiffElement>)super.getValue();
    }
    private static Map<String, DiffElement> emptyElements() { return new HashMap<>(); }

    private DiffNode(final Map<String, DiffElement> elements) {
        super(elements);
    }

    private DiffNode(final DiffElement binding, final Map<String, DiffElement> elements) {
        super(binding, elements);
    }

    public static DiffNode rooted(final String name) {
        return empty(name, DiffElement.ROOT);
    }

    public static DiffNode empty(final String name, final DiffElement parent) {
        final DiffNode df = new DiffNode(emptyElements());
        final DiffElement elt = new DiffElement(name, parent, df);
        df.setBinding(elt);
        return df;
    }

    public static DiffNode empty(String name, DiffValue parent) {
        return empty(name, parent.getBinding());
    }

    @Override
    public boolean isAtomic() { return false; }

    public Collection<String> getElementNames() {
        return getElementMap().keySet();
    }

    public Collection<DiffElement> getElements() {
        return getElementMap().values();
    }

    private List<DiffElement> getElements(boolean atomicOnly) {
        final List<DiffElement> elts = new ArrayList<>();
        for ( final DiffElement elt : getElements() )
            if ( (atomicOnly && elt.getValue().isAtomic()) || (! atomicOnly && elt.getValue().isCompound())) {
                elts.add(elt);
            }
        return elts;
    }

    public List<DiffElement> getAtomicElements() {
        return getElements(true);
    }

    public List<DiffElement> getCompoundElements() {
        return getElements(false);
    }

    /**
     * Returns the element bound to name, or null if no such binding exists
     * @param name
     * @return
     */
    public DiffElement getElement(String name) {
        return getElementMap().get(name);
    }

    /**
     * Returns true if name is bound in this node
     * @param name
     * @return
     */
    public boolean hasElement(String name) {
        return getElement(name) != null;
    }

    public void add(final DiffElement elt) {
        if ( getElementMap().containsKey(elt.getName()) ) {
            throw new IllegalArgumentException("Attempting to rebind already existing binding: " + elt + " node=" + this);
        }
        getElementMap().put(elt.getName(), elt);
    }

    public void add(final DiffValue elt) {
        add(elt.getBinding());
    }

    public void addAll(Collection<DiffElement> elts) {
        for ( final DiffElement e : elts ) {
            add(e);
        }
    }

    public void add(String name, Object value) {
        add(new DiffElement(name, this.getBinding(), new DiffValue(value)));
    }

    @Override
    public int size() {
        int count = 0;
        for ( final DiffElement value : getElements() ) {
            count += value.size();
        }
        return count;
    }

    @Override
    public String toString() {
        return toString(0);
    }

    @Override
    public String toString(int offset) {
        String off = offset > 0 ? Strings.repeat(" ", offset) : "";
        StringBuilder b = new StringBuilder();

        b.append("(").append("\n");
        Collection<DiffElement> atomicElts = getAtomicElements();
        for ( DiffElement elt : atomicElts ) {
            b.append(elt.toString(offset + 2)).append('\n');
        }

        for ( DiffElement elt : getCompoundElements() ) {
            b.append(elt.toString(offset + 4)).append('\n');
        }
        b.append(off).append(")").append("\n");

        return b.toString();
    }

    @Override
    public String toOneLineString() {
        StringBuilder b = new StringBuilder();

        b.append('(');
        List<String> parts = new ArrayList<>();
        for ( DiffElement elt : getElements() )
            parts.add(elt.toOneLineString());
        b.append(Utils.join(" ", parts));
        b.append(')');

        return b.toString();
    }

    public List<Difference> diff(final DiffNode test) {
        final Set<String> allNames = new HashSet<>(this.getElementNames());
        allNames.addAll(test.getElementNames());
        final List<Difference> diffs = new ArrayList<>();

        for ( final String name : allNames ) {
            final DiffElement masterElt = this.getElement(name);
            final DiffElement testElt = test.getElement(name);
            if ( masterElt == null && testElt == null ) {
                throw new GATKException("BUG: unexpectedly got two null elements for field: " + name);
            } else if ( masterElt == null || testElt == null ) { // if either is null, we are missing a value
                // todo -- should one of these be a special MISSING item?
                diffs.add(new Difference(masterElt, testElt));
            } else {
                diffs.addAll(masterElt.diff(testElt));
            }
        }

        return diffs;
    }
}
