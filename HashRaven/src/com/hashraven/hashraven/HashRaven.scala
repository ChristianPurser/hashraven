package com.hashraven.hashraven
// HashRaven
// Originally by Christian Purser

import scala.io.Source
import java.io.File
import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import java.security.MessageDigest

object HashRaven {
  var hashesFound = 0l
  var outputFilename = ""
  var runningLocal = true
  var desiredCharacters = ""
  var charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~".toCharArray()
  var charsets = collection.mutable.Map[String, Array[Char]]()
  charsets += ("l" -> "abcdefghijklmnopqrstuvwxyz".toCharArray())
  charsets += ("u" -> "ABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray())
  charsets += ("d" -> "0123456789".toCharArray())
  charsets += ("h" -> "0123456789abcdef".toCharArray())
  charsets += ("H" -> "0123456789ABCDEF".toCharArray())
  charsets += ("s" -> " !\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~".toCharArray())    
  charsets += ("a" -> "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 !\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~".toCharArray())

  def main(args: Array[String]) {
    if (args.length > 0) {
      for(i <- 0 until args.length) {
        if (args(i).equals("server")) {
          runningLocal = false
        }
      }
      
      var operationPerformed = false
      for(i <- 0 until args.length) {
        if (args(i).equals("test") && !operationPerformed) {
          // Test Suite
          var fileHandle = new java.io.PrintWriter(new File("data/sample.hsh"))
          fileHandle.write(DigestUtils.md5Hex("goldfish4") + "\r\n")
          fileHandle.write(DigestUtils.md5Hex("well") + "\r\n")
          fileHandle.write(DigestUtils.md5Hex("Green") + "\r\n")
          fileHandle.close
            
          fileHandle = new java.io.PrintWriter(new File("data/sample.dct"))
          fileHandle.write("goldfish" + "\r\n")
          fileHandle.close
      
          fileHandle = new java.io.PrintWriter(new File("data/sample.rul"))
          fileHandle.write("$4" + "\r\n")
          fileHandle.close

          rulesAttack(runningLocal)    

          fileHandle = new java.io.PrintWriter(new File("data/sample.msk"))
          fileHandle.write("llll" + "\r\n")
          fileHandle.write("ullll" + "\r\n")
          fileHandle.close

          bruteAttack(runningLocal)    

          generateList()
          operationPerformed = true
        } else if (args(i).equals("generate")) {
          generateRandomRules(args(i + 1).toInt)
          operationPerformed = true
        } else if (args(i).equals("rules")) {
          rulesAttack(runningLocal)
          generateList()
          operationPerformed = true
        } else if (args(i).equals("brute")) {
          bruteAttack(runningLocal)
          generateList()
          operationPerformed = true
        } else if (args(i).equals("learn")) {
          while (true) {
            generateRandomRules(args(i + 1).toInt)
            rulesAttack(runningLocal)
            generateList()
            operationPerformed = true
          }
        } else if (args(i).equals("combined")) {
          bruteAttack(runningLocal)
          rulesAttack(runningLocal)
          generateList()
        }
      }
    } else {
      System.out.println("hashRaven requires arguments and always operates on all files in /data:")
      System.out.println("test - tests program (Expected: 4 Found) and generates example data demonstrating required forms)")
      System.out.println("generate # - generates # random rules")
      System.out.println("rules - tests against rules")
      System.out.println("brute - brute force hash using masks")
      System.out.println("learn # - generates # rules, tests them and orders them, endlessly")
      System.out.println("combined - brutes masks against hashes, then tests rules against hashes")
      System.out.println("args: server - sets context to yarn (untested)")
    }
  }
  
  def getListOfFiles(dir: File):List[File] = dir.listFiles.filter(_.isFile).toList
  
  def generateRandomRules(quantity:Int) {
    // Generate random String of Rule Items
    var ruleList = scala.collection.mutable.Buffer[String]()
    for(i <- 0 until quantity) {
      var numberOfRules = Math.ceil(5 * Math.random()).toInt;
      var thisRule = ""
      
      for(rule <- 0 until numberOfRules) {
        var randomRoll = Math.floor(29 * Math.random()).toInt;
        if (randomRoll == 0) {thisRule += ":"}
        else if (randomRoll == 1) {thisRule += "l"}
        else if (randomRoll == 2) {thisRule += "u"}
        else if (randomRoll == 4) {thisRule += "c"}
        else if (randomRoll == 5) {thisRule += "C"}
        else if (randomRoll == 6) {thisRule += "t"}
        else if (randomRoll == 7) {thisRule += "T" + Math.ceil(9 * Math.random()).toInt}
        else if (randomRoll == 8) {thisRule += "r"}
        else if (randomRoll == 9) {thisRule += "d"}
        else if (randomRoll == 10) {thisRule += "p" + 1}
        else if (randomRoll == 11) {thisRule += "f"}
        else if (randomRoll == 12) {thisRule += "{"}
        else if (randomRoll == 13) {thisRule += "}"}
        else if (randomRoll == 14) {thisRule += "$" + Math.ceil(9 * Math.random()).toInt}
        else if (randomRoll == 15) {thisRule += "^" + Math.ceil(9 * Math.random()).toInt}
        else if (randomRoll == 16) {thisRule += "["}
        else if (randomRoll == 17) {thisRule += "]"}
        else if (randomRoll == 18) {thisRule += "D" + Math.ceil(9 * Math.random()).toInt}
        else if (randomRoll == 19) {thisRule += "x" + Math.ceil(9 * Math.random()).toInt + Math.ceil(9 * Math.random()).toInt}
        else if (randomRoll == 20) {thisRule += "O" + Math.ceil(9 * Math.random()).toInt + Math.ceil(9 * Math.random()).toInt}
        else if (randomRoll == 21) {thisRule += "i" + Math.ceil(9 * Math.random()).toInt + charset(Math.floor(charset.length * Math.random()).toInt)}
        else if (randomRoll == 22) {thisRule += "o" + Math.ceil(9 * Math.random()).toInt + charset(Math.floor(charset.length * Math.random()).toInt)}
        else if (randomRoll == 23) {thisRule += "'" + Math.ceil(9 * Math.random()).toInt}
        else if (randomRoll == 24) {thisRule += "s" + charset(Math.floor(charset.length * Math.random()).toInt) + charset(Math.floor(charset.length * Math.random()).toInt)}
        else if (randomRoll == 25) {thisRule += "@" + charset(Math.floor(charset.length * Math.random()).toInt)}
        else if (randomRoll == 26) {thisRule += "z" + Math.ceil(5 * Math.random()).toInt}
        else if (randomRoll == 27) {thisRule += "Z" + Math.ceil(5 * Math.random()).toInt}
        else if (randomRoll == 28) {thisRule += "q"}
        
        if (rule != numberOfRules) {thisRule += " "}        
      }      

      ruleList += thisRule
    }
    
    // Save Rule File
    val pw = new java.io.PrintWriter(new File("data/random-" + System.currentTimeMillis() + ".rul"))
    for(i <- 0 until ruleList.length) {
      pw.write(ruleList(i) + "\r\n")      
    }
    pw.close
  }    
  
  def generateList() {
    // Load Rules
    val ruleSetList = new java.io.File("data/").listFiles.filter(_.getName.endsWith(".rul"))
    var ruleList = scala.collection.mutable.Buffer[String]()

    for(i <- 0 until ruleSetList.length) {
      for (line <- Source.fromFile(ruleSetList(i)).getLines) {
        ruleList += line
      }
    }    

    // Load Results
    val resultsListList = new java.io.File("data/").listFiles.filter(_.getName.endsWith(".rsl"))
    var resultsList = scala.collection.mutable.Buffer[String]()

    for(i <- 0 until resultsListList.length) {
      for (line <- Source.fromFile(resultsListList(i)).getLines) {
        // Add to Results if thre are Three Fields
        val resultsArray = line.split(":")
        if (resultsArray.size == 3) {
          resultsList += line
        }
      }
    }
    
    // format hash:plaintext:rule
        
    // Create Rule Order from Results
    var ruleOrder = scala.collection.mutable.Buffer[String]()
    for(outerRule <- 0 until ruleList.length) {
      
      // Declare Hits Collection
      var ruleHits = scala.collection.mutable.Buffer[Int]()
      for(innerRule <- 0 until ruleList.length) {
        ruleHits += 0
      }
            
      // Tally Up Hits
      for(innerRule <- 0 until ruleList.length) {
        for(result <- 0 until resultsList.length) {
          val resultsArray = resultsList(result).split(":")
          if (resultsArray(2).equals(ruleList(innerRule))) {
            ruleHits(innerRule) += 1
          }
        }
      }
      
      // Find Best Rule
      var bestRule = "";
      var bestRuleCount = 0;
      for(bestRuleCheck <- 0 until ruleHits.length) {
        if (ruleHits(bestRuleCheck) > bestRuleCount) {
          bestRule = ruleList(bestRuleCheck)
          bestRuleCount = ruleHits(bestRuleCheck)
        }
      }

      ruleOrder += bestRule;

      // Remove All Entries Solved by Best Rule
      var hashesBroken  = scala.collection.mutable.Buffer[String]()
      for(result <- 0 until resultsList.length) {
        val resultsArray = resultsList(result).split(":")
        if (resultsArray(2).equals(bestRule)) {
          for(result <- 0 until resultsList.length) {
            hashesBroken += resultsArray(0)
          }
        }
        
        for(result <- 0 until resultsList.length) {
          val resultsArray = resultsList(result).split(":")
          for(brokenHash <- 0 until hashesBroken.length) {
            if (resultsArray(0).equals(hashesBroken(brokenHash))) {
              resultsList(result) = "null:null:null";
            }
          }
        }
      }
    }
    
    // Save Rule List (format rule)
    val pw = new java.io.PrintWriter(new File("data/ordered-" + System.currentTimeMillis() + ".rul"))
    for(i <- 0 until ruleOrder.length) {
      pw.write(ruleOrder(i) + "\r\n")
    }
    pw.close
  }
  
  // Brute Force
  def bruteAttack(runningLocal:Boolean) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine or yarn if specified
    var contextString = "local[*]"
    if (!runningLocal) {contextString = "yarn"}
    val sparkContext = new SparkContext(contextString, "HashRaven") 
    
    // Loads Hashes
    System.out.println("Loading Hashes")
    val dbList = new java.io.File("data/").listFiles.filter(_.getName.endsWith(".hsh"))
    var hashSortedHashes = scala.collection.mutable.Map[String, Boolean]()
    
    for(i <- 0 until dbList.length) {
      for (line <- Source.fromFile(dbList(i))(scala.io.Codec.ISO8859).getLines) {
        val hashesArray = line.split(":")
        for (i <- 0 until hashesArray.length) {
          if (hashesArray(i).length() == 32) {
            if (hashSortedHashes.contains(hashesArray(i))) {
              
            } else {
              hashSortedHashes(hashesArray(i)) = true
            }
          }
        }
      }
    }

    var broadcastHashes = sparkContext.broadcast(hashSortedHashes)

    // Load Mask Rules
    System.out.println("Loading Mask Rules")
    var maskList = scala.collection.mutable.Buffer[String]()
    val maskSetList = new java.io.File("data/").listFiles.filter(_.getName.endsWith(".msk"))

    for(i <- 0 until maskSetList.length) {
      for (line <- Source.fromFile(maskSetList(i))(scala.io.Codec.ISO8859).getLines) {
        maskList += line
      }
    }
    
    // Create New Results File
    outputFilename = "data/result-" + System.currentTimeMillis() + ".rsl" 
    
    // Check Each Mask 
    var mask = 0
    while (mask < maskList.length) {
      var desiredCharacters = maskList(mask)

      if (desiredCharacters.length >= 3) {
        // Attempt Hashes
        System.out.println("Beginning Brute Force Hash")
        System.out.println("Brute Forcing " + desiredCharacters + " Mask")
        val startingTime = System.currentTimeMillis()
        var lastTime = startingTime
        var hashesHashedTotal = 0l
    
        // Make a List of All Three Character Combinations
        var bruteArray = scala.collection.mutable.Buffer[String]()
        
        var position = 0
        var innerPosition = 0
        var bruteString = ""
        while (position < charsets(desiredCharacters(0).toString()).length) {
          var innerPosition = 0
          
          while (innerPosition < charsets(desiredCharacters(1).toString()).length) {
            var innermostPosition = 0
    
            while (innermostPosition < charsets(desiredCharacters(2).toString()).length) {
              bruteArray += "" + charsets(desiredCharacters(0).toString())(position) + charsets(desiredCharacters(1).toString())(innerPosition) + charsets(desiredCharacters(2).toString())(innermostPosition)
              
              innermostPosition += 1
            }
            
            innerPosition += 1
          }
          
          position += 1
        }
    
        val bruteWords = sparkContext.parallelize(bruteArray)        
            
        // Brute Force
        bruteWords.foreach(thisWord => bruteWordWithCPU(thisWord, broadcastHashes, desiredCharacters))   
        System.out.println(desiredCharacters + " Mask Brute Force Completed: " + hashesFound + " Found (Total)")      
        
        // Done Brute Forcing
        val endingTime = System.currentTimeMillis() - startingTime
        System.out.println("Hashing Complete (" + endingTime + " MS Elapsed)")
        
        mask += 1
      } else {
        System.out.println("Skipped a one or two character mask.")
      }
    }
    // Close the Spark Context
    sparkContext.stop()
  }
  
  def rulesAttack(runningLocal:Boolean) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine or yarn if specified
    var contextString = "local[*]"
    if (!runningLocal) {contextString = "yarn"}
    val sparkContext = new SparkContext(contextString, "HashRaven") 

    // Loads Hashes
    System.out.println("Loading Hashes")
    val dbList = new java.io.File("data/").listFiles.filter(_.getName.endsWith(".hsh"))
    var hashSortedHashes = scala.collection.mutable.Map[String, Boolean]()
    
    for(i <- 0 until dbList.length) {
      for (line <- Source.fromFile(dbList(i))(scala.io.Codec.ISO8859).getLines) {
        val hashesArray = line.split(":")
        for (i <- 0 until hashesArray.length) {
          if (hashesArray(i).length() == 32) {
            if (hashSortedHashes.contains(hashesArray(i))) {
              
            } else {
              hashSortedHashes(hashesArray(i)) = true
            }
          }
        }
      }
    }

    var broadcastHashes = sparkContext.broadcast(hashSortedHashes)

    // Load Dicts as Lines
    System.out.println("Loading Dictionaries as Lines")
    val rawDictLines = sparkContext.textFile("data/*.dct")
    System.out.println(rawDictLines.count() + " Dictionary Entries Loaded")
    
    // Preprocess List
    val dictLines = rawDictLines.filter(thisLine => thisLine.length() >= 8) 
    System.out.println(dictLines.count() + " Dictionary Entries After Preprocessing")
    dictLines.cache()
    
    // Load Rules with Preprocessing
    System.out.println("Loading Rules")
    var ruleList = scala.collection.mutable.Buffer[String]()
    val ruleSetList = new java.io.File("data/").listFiles.filter(_.getName.endsWith(".rul"))

    for(i <- 0 until ruleSetList.length) {
      for (line <- Source.fromFile(ruleSetList(i))(scala.io.Codec.ISO8859).getLines) {
        // Preprocessing
        var preprocessedLine = line
        
        // Remove Spaces
        preprocessedLine = preprocessedLine.replace(" ", "")

        // Preprocess All Commands
        var thisPosition = 0
        var finalLine = ""
        while (thisPosition < preprocessedLine.length()) {
          preprocessedLine.charAt(thisPosition) match {
            case ':' => 
            case 'l' => finalLine += preprocessedLine.charAt(thisPosition).toString() + " "
            case 'u' => finalLine += preprocessedLine.charAt(thisPosition).toString() + " "
            case 'c' => finalLine += preprocessedLine.charAt(thisPosition).toString() + " "
            case 'C' => finalLine += preprocessedLine.charAt(thisPosition).toString() + " "
            case 't' => finalLine += preprocessedLine.charAt(thisPosition).toString() + " "
            case 'T' => 
              if (preprocessedLine.charAt(thisPosition + 1).isDigit) {
                finalLine += preprocessedLine.charAt(thisPosition).toString() + preprocessedLine.charAt(thisPosition + 1).toString() + " "
                thisPosition += 1
              }
            case 'r' => finalLine += preprocessedLine.charAt(thisPosition) + " "
            case 'd' => finalLine += preprocessedLine.charAt(thisPosition) + " "
            case 'p' => 
              if (preprocessedLine.charAt(thisPosition + 1).isDigit) {
                finalLine += preprocessedLine.charAt(thisPosition).toString() + preprocessedLine.charAt(thisPosition + 1).toString() + " "
                thisPosition += 1
              }
            case 'f' => finalLine += preprocessedLine.charAt(thisPosition) + " "
            case '{' => finalLine += preprocessedLine.charAt(thisPosition) + " "
            case '}' => finalLine += preprocessedLine.charAt(thisPosition) + " "
            case '$' => 
                finalLine += preprocessedLine.charAt(thisPosition).toString() + preprocessedLine.charAt(thisPosition + 1).toString() + " "
                thisPosition += 1
            case '^' =>
                finalLine += preprocessedLine.charAt(thisPosition).toString() + preprocessedLine.charAt(thisPosition + 1).toString() + " "
                thisPosition += 1
            case '[' => finalLine += preprocessedLine.charAt(thisPosition).toString() + " "
            case ']' => finalLine += preprocessedLine.charAt(thisPosition).toString() + " "
            case 'D' => 
              if (preprocessedLine.charAt(thisPosition + 1).isDigit) {
                finalLine += preprocessedLine.charAt(thisPosition).toString() + preprocessedLine.charAt(thisPosition + 1).toString() + " "
                thisPosition += 1
              }
            case 'x' => 
              if (preprocessedLine.charAt(thisPosition + 1).isDigit && preprocessedLine.charAt(thisPosition + 2).isDigit) {
                finalLine += preprocessedLine.charAt(thisPosition).toString() + preprocessedLine.charAt(thisPosition + 1).toString() + preprocessedLine.charAt(thisPosition + 2).toString() + " "
                thisPosition += 2
              }
            case 'O' => 
              if (preprocessedLine.charAt(thisPosition + 1).isDigit && preprocessedLine.charAt(thisPosition + 2).isDigit) {
                finalLine += preprocessedLine.charAt(thisPosition).toString() + preprocessedLine.charAt(thisPosition + 1).toString() + preprocessedLine.charAt(thisPosition + 2).toString() + " "
                thisPosition += 2
              }
            case 'i' => 
              if (preprocessedLine.charAt(thisPosition + 1).isDigit) {
                finalLine += preprocessedLine.charAt(thisPosition).toString() + preprocessedLine.charAt(thisPosition + 1).toString() + preprocessedLine.charAt(thisPosition + 2).toString() + " "
                thisPosition += 2
              }
            case 'o' => 
              if (preprocessedLine.charAt(thisPosition + 1).isDigit) { 
                finalLine += preprocessedLine.charAt(thisPosition).toString() + preprocessedLine.charAt(thisPosition + 1).toString() + preprocessedLine.charAt(thisPosition + 2).toString() + " "
                thisPosition += 2
              }
            case ''' => 
              if (preprocessedLine.charAt(thisPosition + 1).isDigit) {
                finalLine += preprocessedLine.charAt(thisPosition).toString() + preprocessedLine.charAt(thisPosition + 1).toString() + " "
                thisPosition += 1
              }
            case 's' =>
                finalLine += preprocessedLine.charAt(thisPosition).toString() + preprocessedLine.charAt(thisPosition + 1).toString() + preprocessedLine.charAt(thisPosition + 2).toString() + " "
                thisPosition += 2
            case '@' =>
                finalLine += preprocessedLine.charAt(thisPosition).toString() + preprocessedLine.charAt(thisPosition + 1).toString() + " "
                thisPosition += 1
            case 'z' => 
              if (preprocessedLine.charAt(thisPosition + 1).isDigit) {
                finalLine += preprocessedLine.charAt(thisPosition).toString() + preprocessedLine.charAt(thisPosition + 1).toString() + " "
                thisPosition += 1
              }
            case 'Z' => 
              if (preprocessedLine.charAt(thisPosition + 1).isDigit) {
                finalLine += preprocessedLine.charAt(thisPosition).toString() + preprocessedLine.charAt(thisPosition + 1).toString() + " "
                thisPosition += 1
              }
            case 'q' => finalLine += preprocessedLine.charAt(thisPosition).toString() + " "
            case _ => System.out.println(preprocessedLine.charAt(thisPosition) + "@" + thisPosition + " skipped in " + preprocessedLine)
          }
          
          thisPosition += 1
        }

        // Strip Trailing Space
        finalLine = finalLine.dropRight(1)
        
        ruleList += finalLine        
      }
    }

    System.out.println(ruleList.length + " Rule Sets Loaded")
    
    // Create New Results File
    outputFilename = "data/result-" + System.currentTimeMillis() + ".rsl" 
    
    // Attempt Hashes
    System.out.println("Beginning Hashing against " + ruleList.length.toLong + " Rules")
    val startingTime = System.currentTimeMillis()
    var lastTime = startingTime
    var hashesHashedTotal = 0l
        
    var rule = 0
    while (rule < ruleList.length) {  
      // Split the Rule into Rules
      var rules = ruleList(rule).split("\\s+")
      
      // Test this Rule against each DictLine
      dictLines.foreach(thisLine => hashLineWithCPU(thisLine, rules, broadcastHashes))
                  
      // Print Progress After Each Rule
      val timeElapsed = (System.currentTimeMillis() - lastTime)
      var stringOut = "One Rule Tried Against All Dictionaries and Hashes in " + timeElapsed + " MS (Finished: " + (rule + 1) + " / " + ruleList.length + ") - (" + hashesFound + " Found)"

      lastTime = System.currentTimeMillis()
      
      var secondsPerRule =  ((lastTime.toLong - startingTime.toLong)) / (rule + 1)
      var estimatedSeconds = ((ruleList.length - (rule + 1)) * secondsPerRule) / 1000L
      var estimatedMinutes = estimatedSeconds.toLong / 60L; estimatedSeconds -= estimatedMinutes.toLong  * 60L
      var estimatedHours = estimatedMinutes.toLong / 60L; estimatedMinutes -= estimatedHours.toLong  * 60L
      var estimatedDays = estimatedHours.toLong / 24L; estimatedHours -= estimatedDays.toLong  * 24L
      var estimatedYears = estimatedDays.toLong / 356L; estimatedDays -= estimatedYears.toLong  * 356L
      stringOut += " - Time Left: " + estimatedYears + "y:" + estimatedDays + "d:" + estimatedHours  + "h:" + estimatedMinutes  + "m:" + estimatedSeconds + "s Left)"
      System.out.println("")
      System.out.println(stringOut)

      var thisRule = 0
      while (thisRule < rules.length) {
        System.out.println(rules(thisRule))
        thisRule += 1
      }

      rule += 1
    }

    // Close the Spark Context
    sparkContext.stop()

    val endingTime = System.currentTimeMillis() - startingTime
    System.out.println("Hashing Complete (" + endingTime + " MS Elapsed)")
  }

  // Brute Hashing Function (CPU)
  def bruteWordWithCPU(word:String, broadcastHashes:org.apache.spark.broadcast.Broadcast[scala.collection.mutable.Map[String, Boolean]], desiredCharacters:String) {  
    // Get the Word
    var bruteString = word
    
    // Declare an Int Array
    var bruteCharacterArray = new Array[Integer](desiredCharacters.length() - 3)
    
    var thisPosition = 0
    while (thisPosition < bruteCharacterArray.length) {
      bruteCharacterArray(thisPosition) = 0
        
      thisPosition += 1
    }
    
    var done = false
    while (!done) {
      // Compile the String
      var bruteCharacters = ""
      
      thisPosition = 0
      while (thisPosition < bruteCharacterArray.length) {
        bruteCharacters += charsets(desiredCharacters(thisPosition + 3).toString())(bruteCharacterArray(thisPosition))
        
        thisPosition += 1
      }
      
      // Hash the Result (Apache)
      val hashedWord = DigestUtils.md5Hex(bruteString + bruteCharacters)
      
      // Check the Hash
      if (broadcastHashes.value.contains(hashedWord)) {
        // Generate Rules String
        var rulesString = desiredCharacters
        
        val resultString = hashedWord + ":" + bruteString + bruteCharacters + ":" + rulesString // format hash:plaintext:rule
        val fileHandle = new java.io.PrintWriter(new File(outputFilename))
        fileHandle.write(resultString + "\r\n")      
        fileHandle.close
        hashesFound += 1
      }
      
      
      if (bruteCharacterArray.length > 0) {
        // Advance Word
        bruteCharacterArray(0) += 1
        thisPosition = 0
        while (thisPosition < bruteCharacterArray.length) {
          if (bruteCharacterArray(thisPosition) >= charsets(desiredCharacters(thisPosition + 3).toString()).length) {
            if (bruteCharacterArray.length > thisPosition + 1) {
              bruteCharacterArray(thisPosition + 1) += 1
              bruteCharacterArray(thisPosition) = 0
            } else {
              done = true
            }
          }
          
          thisPosition += 1
        }
      } else {
        done = true
      }
    }        
  }

  // Line Hashing Function (CPU)
  def hashLineWithCPU(word:String, rules:Array[String], broadcastHashes:org.apache.spark.broadcast.Broadcast[scala.collection.mutable.Map[String, Boolean]]) {
    // Get the Word
    val thisWord = getWord(rules, word)
    
    // Hash the Result (Apache)
    val hashedWord = DigestUtils.md5Hex(thisWord)
          
    // Check the Hash
    if (broadcastHashes.value.contains(hashedWord)) {
      // Generate Rules String
      var rulesString = ""
      var thisRule = 0
      while (thisRule < rules.length) {
        if (thisRule > 0) {
          rulesString += " "
        }
        
        rulesString += rules(thisRule)
        
        thisRule += 1
      }
      
      
      val resultString = hashedWord + ":" + thisWord + ":" + rulesString // format hash:plaintext:rule
      val fileHandle = new java.io.PrintWriter(new File(outputFilename))
      fileHandle.write(resultString + "\r\n")      
      fileHandle.close
      hashesFound += 1
    }
  }

  // Calculate Word
  def getWord(rules:Array[String], word:String):String = {
    // Apply Rule to Word
    var editableWord = word
    
    var thisRule = 0
    while (thisRule < rules.length) {
      if (rules(thisRule).length() > 0) {
        rules(thisRule).charAt(0) match {
          case ':' => 
          case 'l' => editableWord = editableWord.toLowerCase() // Lower Case All Letters
          case 'u' => editableWord = editableWord.toUpperCase() // Upper Case All Letters
          case 'c' =>         
            if (editableWord.length > 0) {
              editableWord = editableWord(0).toUpper + editableWord.substring(1).toLowerCase()
            } // Capitalize First Letter, Lower Case All Others
          case 'C' =>         
            if (editableWord.length > 0) {
              editableWord = editableWord(0).toLower + editableWord.substring(1).toUpperCase()
            } // Lowercase First Letter, Upper Case All Others
          case 't' =>
            val newWord = ""; 
            for (letter <- 0 until editableWord.length) {
              if (editableWord(letter).isUpper) {
                newWord + editableWord(letter).toLower
              } else if (editableWord(letter).isLower) {
                newWord + editableWord(letter).toUpper
              } else {
                newWord + editableWord(letter)
              }
            }
            editableWord = newWord; // Toggle All Cases ()
          case 'T' =>
            if (editableWord.length > rules(thisRule).charAt(1).toInt) {
              val newWord = editableWord.substring(0, rules(thisRule).charAt(1).toInt); 
              if (editableWord(rules(thisRule).charAt(1)).isUpper) {
                newWord + editableWord(rules(thisRule).charAt(1)).toLower
              } else if (editableWord(rules(thisRule).charAt(1)).isLower) {
                newWord + editableWord(rules(thisRule).charAt(1)).toUpper
              } else {
                newWord + editableWord(rules(thisRule).charAt(1))
              }
              editableWord = newWord + editableWord.substring(rules(thisRule).charAt(1).toInt + 1, editableWord.length());  // Toggle Case at Position N
            }
          case 'r' => editableWord = editableWord.reverse // Reverse the Word
          case 'd' => editableWord = editableWord + editableWord // Duplicate the Word
          case 'p' => val originalWord = editableWord; for (thisRule <- 0 until rules(thisRule).charAt(1).toInt) {editableWord = editableWord + originalWord} // Append Word N Times
          case 'f' => editableWord = editableWord + editableWord.reverse // Duplicate the Word Reversed
          case '{' => if (editableWord.length > 0) {editableWord = editableWord.substring(1) + editableWord.substring(0)} // Move First Character to End
          case '}' => if (editableWord.length > 0) {editableWord = editableWord.substring(editableWord.length - 1) + editableWord.dropRight(1)} // Move Last Character to Beginning
          case '$' => editableWord = editableWord + rules(thisRule).charAt(1) // Append Character X to End
          case '^' => editableWord = rules(thisRule).charAt(1) + editableWord // Prepend Character X to Beginning
          case '[' => if (editableWord.length > 0) {editableWord = editableWord.substring(1)} // Delete First Character
          case ']' => editableWord = editableWord.dropRight(1) // Delete Last Character
          case 'D' => 
            if (editableWord.length > rules(thisRule).charAt(1).toInt) {
              editableWord = editableWord.substring(0, rules(thisRule).charAt(1).toInt) + editableWord.substring(rules(thisRule).charAt(1).toInt + 1, editableWord.length())
            } // Delete Character at Position N
          case 'x' => 
            if (editableWord.length > rules(thisRule).charAt(1).toInt + rules(thisRule).charAt(2).toInt) {
              if (rules(thisRule).charAt(1).toInt > rules(thisRule).charAt(2).toInt) {
                editableWord = editableWord.substring(rules(thisRule).charAt(2).toInt, rules(thisRule).charAt(1).toInt)
              } else {
                editableWord = editableWord.substring(rules(thisRule).charAt(1).toInt, rules(thisRule).charAt(2).toInt)
              }
            } // Keeps only N characters starting at M
          case 'O' => 
            if (editableWord.length > rules(thisRule).charAt(1).toInt + rules(thisRule).charAt(2).toInt) {
              editableWord = editableWord.substring(0, rules(thisRule).charAt(2).toInt - 1) + editableWord.substring(rules(thisRule).charAt(2).toInt + rules(thisRule).charAt(1).toInt,editableWord.length)  
            } // Delete N characters starting at M
          case 'i' =>
            if (editableWord.length > rules(thisRule).charAt(1).toInt) {
              editableWord = editableWord.substring(0, rules(thisRule).charAt(1).toInt) + rules(thisRule).charAt(2) + editableWord.substring(rules(thisRule).charAt(1).toInt, editableWord.length())  
            } // Insert Character X at Position N
          case 'o' =>
            if (editableWord.length > rules(thisRule).charAt(1).toInt) {
              editableWord = editableWord.substring(0, rules(thisRule).charAt(1).toInt) + rules(thisRule).charAt(2) + editableWord.substring(rules(thisRule).charAt(1).toInt + 1, editableWord.length())
            } // Overwrite Character X at Position N
          case ''' =>
            if (editableWord.length > rules(thisRule).charAt(1).toInt) {
              editableWord = editableWord.substring(0, rules(thisRule).charAt(1).toInt - 1)
            } // Delete Characters starting at Position N
          case 's' =>
            var newWord = ""
            for (letter <- 0 until editableWord.length) {
              if (editableWord(letter).equals(rules(thisRule).charAt(1))) {
                newWord += rules(thisRule).charAt(2)
              } else {
                newWord += editableWord(letter)
              }
            }        
            editableWord = newWord // Replace All Instances of X with Y
          case '@' =>
            var newWord = ""; 
            for (letter <- 0 until editableWord.length) {
              if (editableWord(letter).equals(rules(thisRule).charAt(1))) {
                
              } else {
                newWord += editableWord(letter)
              }
            }        
            editableWord = newWord // Delete All Instances of X
          case 'z' => if (editableWord.length > 0) {for (thisRule <- 0 until rules(thisRule).charAt(1).toInt) {editableWord = editableWord(0) + editableWord}} // Duplicate First Character N Times
          case 'Z' => if (editableWord.length > 0) {for (thisRule <- 0 until rules(thisRule).charAt(1).toInt) {editableWord = editableWord + editableWord(editableWord.length - 1)}} // Duplicate Last Character N Times
          case 'q' => 
            var newWord = ""; 
            for (letter <- 0 until editableWord.length) {
              newWord += editableWord(letter) + editableWord(letter)
            }
            editableWord = newWord // Duplicate Every Character   
        }
      }
      
      thisRule += 1  
    }
        
    return editableWord
  }
}