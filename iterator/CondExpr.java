package iterator;
import java.lang.*;
import java.io.*;
import global.*;

/**
 *  This clas will hold single select condition
 *  It is an element of linked list which is logically
 *  connected by OR operators.
 */

public class CondExpr {
  
  /**
   * Operator like "<"
   */
  public AttrOperator op;    
  
  /**
   * Types of operands, Null AttrType means that operand is not a
   * literal but an attribute name
   */    
  public AttrType     type1;
  public AttrType     type2;    
 
  /**
   *the left operand and right operand 
   */ 
  public Operand operand1;
  public Operand operand2;
  
  /**
   * Pointer to the next element in linked list
   */    
  public CondExpr    next;   
  
  /**
   *constructor
   */
  public  CondExpr() {
    
    operand1 = new Operand();
    operand2 = new Operand();
    
    operand1.integer = 0;
    operand2.integer = 0;
    
    next = null;
  }

  public CondExpr(AttrOperator attrOperator, AttrType type1, AttrType type2, FldSpec operand1Symbol, String operand2String) {
    operand1 = new Operand();
    operand2 = new Operand();

    operand1.integer = 0;
    operand2.integer = 0;

    this.op = attrOperator;
    this.type1 = type1;
    this.type2 = type2;
    this.operand1.symbol = operand1Symbol;
    this.operand2.string = operand2String;

    next = null;
  }
}

