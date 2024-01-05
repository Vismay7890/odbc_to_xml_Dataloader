using System;
using LogLibrary;
using System.Data;
using System.Data.SqlClient;
using dataloader;

class Program
{
    static void Main()
    {
        Logger logger = new Logger();
        DataLoader dataLoader = new DataLoader(logger);
        dataLoader.Run();
    }
}
