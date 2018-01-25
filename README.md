# hashraven
HashRaven is a password recovery and penetration testing tool written in Scala.

HashRaven checks hashes in md5 against dictionaries using masks and rules.  It conforms to HashCat's syntax for masks and rules.  Though it can run on any number of systems using yarn, it only uses CPU processing.

HashRaven orders rules based on effectiveness after running, and can generate random rules.  It can generate rules and and order the results in an endless loop, creating useful new rules given sufficient time and processing power.
