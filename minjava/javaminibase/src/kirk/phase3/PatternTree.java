package phase3;

import java.util.ArrayList;

public class PatternTree{
  private int numNodes;
  private ArrayList<String> relations;
  private ArrayList<String> nodes;
  private ArrayList<int[]> AncestorDescendantRules; // first is Ancestor, second is Decendent
  private ArrayList<int[]> ParentChildRules;    // first is Parent, second Child

  public PatternTree() {
    nodes = new ArrayList<String>();
    relations = new ArrayList<String>();
    AncestorDescendantRules = new ArrayList<int[]>();
    ParentChildRules = new ArrayList<int[]>();
  }

  /**
   * Add a tag to the Pattern Tree
   *
   * @param nodeTag - the name of the node to add
   */
  public void addTag(String nodeTag) {
    nodes.add(nodeTag);
  }

  /**
   * Get a tag from the Pattern Tree
   *
   * @param nodeNum - the number of the node to return the Tag
   */
  public String getTag(int nodeNum) {
    return nodes.get(nodeNum);
  }

  /**
   * Retiurns an ordered ArrayList of Tags
   *
   * @return String[] - an ordered String array of Tags
   */
  public ArrayList getTags() {
    return (ArrayList) nodes.clone();
  }

  /**
   * @param rel - the relation string
   */
  public void addRelation(String rel) {
    relations.add(rel);
  }

  /**
   * @param index - the relation index number
   * @return - the String relationship
   */
  public String getRelation(int index) {
    return relations.get(index);
  }

  /**
   * @return - the number of relationships in the pattern tree
   */
  public int getRelationSize() {
    return relations.size();
  }

  /**
   * Add a Ancestor-Decendent rule to the Pattern Tree
   *
   * @param Ancestor  - Ancestor node index
   * @param Decendent - Decendent node index
   */
  public void addAncestorDescendant(int Ancestor, int Decendent) {
    int[] pair = {Ancestor, Decendent};
    AncestorDescendantRules.add(pair);
  }

  /**
   * Add a Parent-Child rule to the Pattern Tree
   *
   * @param Parent - Parent node index
   * @param Child  - Child node index
   */
  public void addParentChild(int Parent, int Child) {
    int[] pair = {Parent, Child};
    ParentChildRules.add(pair);
  }

  /**
   * @return the number of Ancestor Descendant Rules
   */
  public int getAncestorDescendantRulesSize() {
    return AncestorDescendantRules.size();
  }

  /**
   * @return the number of Parent Child Rules
   */
  public int getParentChildRulesSize() {
    return ParentChildRules.size();
  }

  /**
   * @param ruleIndex - the specified rule from <code>ParentChildRules</code>
   * @return - the Parent tag String in the Parent-Child rule at <code>ruleIndex</code>
   */
  public String getParent(int ruleIndex) {
    // ruleIndex == -1 implies it's a star
    if (ParentChildRules.get(ruleIndex)[0] == -1) {
      return "*";
    }
    return nodes.get(ParentChildRules.get(ruleIndex)[0]);
  }

  /**
   * @param ruleIndex - the specified rule from <code>ParentChildRules</code>
   * @return - the Child tag String in the Parent-Child rule at <code>ruleIndex</code>
   */
  public String getChild(int ruleIndex) {
    // ruleIndex == -1 implies it's a star
    if (ParentChildRules.get(ruleIndex)[1] == -1) {
      return "*";
    }
    return nodes.get(ParentChildRules.get(ruleIndex)[1]);
  }

  /**
   * @param ruleIndex- the specified rule from <code>ParentChildRules</code>
   * @return - the relationship order number in the Parent-Child rule at <code>ruleIndex</code>
   */
  public int getPCrelNum(int ruleIndex) {
    return ParentChildRules.get(ruleIndex)[2];
  }

  /**
   * @param ruleIndex - the specified rule from <code>AncestorDescendantRules</code>
   * @return - the Ancestor tag String in the Ancestor-Descendant rule at <code>ruleIndex</code>
   */
  public String getAncestor(int ruleIndex) {
    // ruleIndex == -1 implies it's a star
    if (AncestorDescendantRules.get(ruleIndex)[0] == -1) {
      return "*";
    }
    return nodes.get(AncestorDescendantRules.get(ruleIndex)[0]);
  }

  /**
   * @param ruleIndex - the specified rule from <code>AncestorDescendantRules</code>
   * @return - the Descendant tag String in the Ancestor-Descendant rule at <code>ruleIndex</code>
   */
  public String getDescendant(int ruleIndex) {
    // ruleIndex == -1 implies it's a star
    if (AncestorDescendantRules.get(ruleIndex)[1] == -1) {
      return "*";
    }
    return nodes.get(AncestorDescendantRules.get(ruleIndex)[1]);
  }

  /**
   * @param ruleIndex- the specified rule from <code>AncestorDescendantRules</code>
   * @return - the relationship order number in the Ancestor-Descendant rule at <code>ruleIndex</code>
   */
  public int getADrelNum(int ruleIndex) {
    return AncestorDescendantRules.get(ruleIndex)[2];
  }

  /**
   * Sets the number of nodes in the pattern tree
   *
   * @param num - the number of nodes in pattern tree
   */
  public void setNumNodes(int num) {
    numNodes = num;
  }

  /**
   * Returns the number of nodes in the pattern tree
   *
   * @return number of nodes
   */
  public int getNumNodes() {
    return numNodes;
  }

  public String toString() {

    String output = "";
    // # of nodes
    output += nodes.size() + "\n";

    // node values
    for (int i = 0; i < nodes.size(); i++) {
      output += nodes.get(i) + "\n";
    }

    // parent-child relations
    for (int i = 0; i < ParentChildRules.size(); i++) {
      output += getParent(i) + "\t\t" + getChild(i) + "\t\tPC\n";
    }

    // ancestor-descendant relations
    for (int i = 0; i < AncestorDescendantRules.size(); i++) {
      output += getAncestor(i) + "\t\t" + getDescendant(i) + "\t\tAD\n";
    }

    return output;
  }
}