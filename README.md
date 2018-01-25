# hashraven
HashRaven is a password recovery and penetration testing tool written in Scala.

HashRaven checks hashes in md5 against dictionaries using masks and rules.  It conforms to HashCat's syntax for masks and rules.  Though it can run on any number of systems using yarn, it only uses CPU processing.

HashRaven orders rules based on effectiveness after running, and can generate random rules and order the results in an endless loop, using machine learning to generate rule lists, given sufficient time and processing power.
