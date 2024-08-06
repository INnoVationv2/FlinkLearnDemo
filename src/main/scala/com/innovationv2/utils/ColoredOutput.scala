package com.innovationv2.utils

object ColoredOutput {
  private val RESET = "\u001B[0m"
  private val RED = "\u001B[31m"
  private val GREEN = "\u001B[32m"
  private val YELLOW = "\u001B[33m"
  private val BLUE = "\u001B[34m"
  private val PURPLE = "\u001B[35m"

  def redPrint(msg: String): Unit = {
    println(s"$RED$msg$RESET")
  }

  def greenPrint(msg: String): Unit = {
    println(s"$GREEN$msg$RESET")
  }

  def yellowPrint(msg: String): Unit = {
    println(s"$YELLOW$msg$RESET")
  }

  def bluePrint(msg: String): Unit = {
    println(s"$BLUE$msg$RESET")
  }

  def purplePrint(msg: String): Unit = {
    println(s"$PURPLE$msg$RESET")
  }
}
