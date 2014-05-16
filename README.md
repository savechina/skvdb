Simple Kev Value DB
=====

a simple key value db base file store


 Usage:

  SDB sdb = SDBFactory.open("d:\\sdb");

  sdb.put("hello", "hello", "hello\nworld");

  String s = sdb.get("hello", "hello", String.class);

  System.out.println(s);

  sdb.close();