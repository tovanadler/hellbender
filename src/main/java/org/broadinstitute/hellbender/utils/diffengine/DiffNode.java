package org.broadinstitute.hellbender.utils.diffengine;

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

    private DiffNode(Map<String, DiffElement> elements) {
        super(elements);
    }

    private DiffNode(DiffElement binding, Map<String, DiffElement> elements) {
        super(binding, elements);
    }

    // ---------------------------------------------------------------------------
    //
    // constructors
    //
    // ---------------------------------------------------------------------------

    public static DiffNode rooted(String name) {
        return empty(name, DiffElement.ROOT);
    }

    public static DiffNode empty(String name, DiffElement parent) {
        DiffNode df = new DiffNode(emptyElements());
        DiffElement elt = new DiffElement(name, parent, df);
        df.setBinding(elt);
        return df;
    }

    public static DiffNode empty(String name, DiffValue parent) {
        return empty(name, parent.getBinding());
    }

    // ---------------------------------------------------------------------------
    //
    // accessors
    //
    // ---------------------------------------------------------------------------

    @Override
    public boolean isAtomic() { return false; }

    public Collection<String> getElementNames() {
        return getElementMap().keySet();
    }

    public Collection<DiffElement> getElements() {
        return getElementMap().values();
    }

    private Collection<DiffElement> getElements(boolean atomicOnly) {
        List<DiffElement> elts = new ArrayList<DiffElement>();
        for ( DiffElement elt : getElements() )
            if ( (atomicOnly && elt.getValue().isAtomic()) || (! atomicOnly && elt.getValue().isCompound()))
                elts.add(elt);
        return elts;
    }

    public Collection<DiffElement> getAtomicElements() {
        return getElements(true);
    }

    public Collection<DiffElement> getCompoundElements() {
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

    // ---------------------------------------------------------------------------
    //
    // add
    //
    // ---------------------------------------------------------------------------

    public void add(DiffElement elt) {
        if ( getElementMap().containsKey(elt.getName()) )
            throw new IllegalArgumentException("Attempting to rebind already existing binding: " + elt + " node=" + this);
        getElementMap().put(elt.getName(), elt);
    }

    public void add(DiffValue elt) {
        add(elt.getBinding());
    }

    public void add(Collection<DiffElement> elts) {
        for ( DiffElement e : elts )
            add(e);
    }

    public void add(String name, Object value) {
        add(new DiffElement(name, this.getBinding(), new DiffValue(value)));
    }

    public int size() {
        int count = 0;
        for ( DiffElement value : getElements() )
            count += value.size();
        return count;
    }

    // ---------------------------------------------------------------------------
    //
    // toString
    //
    // ---------------------------------------------------------------------------

    @Override
    public String toString() {
        return toString(0);
    }

    @Override
    public String toString(int offset) {
        String off = offset > 0 ? Utils.dupString(' ', offset) : "";
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

    // --------------------------------------------------------------------------------
    //
    // fromString and toOneLineString
    //
    // --------------------------------------------------------------------------------

    public static DiffElement fromString(String tree) {
        return fromString(tree, DiffElement.ROOT);
    }

    /**
     * Doesn't support full tree structure parsing
     * @param tree
     * @param parent
     * @return
     */
    private static DiffElement fromString(String tree, DiffElement parent) {
        // X=(A=A B=B C=(D=D))
        String[] parts = tree.split("=", 2);
        if ( parts.length != 2 )
            throw new GATKException("Unexpected tree structure: " + tree);
        String name = parts[0];
        String value = parts[1];

        if ( value.length() == 0 )
            throw new GATKException("Illegal tree structure: " + value + " at " + tree);

        if ( value.charAt(0) == '(' ) {
            if ( ! value.endsWith(")") )
                throw new GATKException("Illegal tree structure.  Missing ): " + value + " at " + tree);
            String subtree = value.substring(1, value.length()-1);
            DiffNode rec = DiffNode.empty(name, parent);
            String[] subParts = subtree.split(" ");
            for ( String subPart : subParts ) {
                rec.add(fromString(subPart, rec.getBinding()));
            }
            return rec.getBinding();
        } else {
            return new DiffValue(name, parent, value).getBinding();
        }
    }
}
