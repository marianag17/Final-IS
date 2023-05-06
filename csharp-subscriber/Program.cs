using Dapr;
using MongoDB.Driver;
using System.Data.SqlClient;
using System.Net;
var builder = WebApplication.CreateBuilder(args);

var app = builder.Build();

// Dapr configurations
app.UseCloudEvents();

string hostName = Dns.GetHostName(); // Retrive the Name of HOST
           // Get the IP
            string myIP = Dns.GetHostByName(hostName).AddressList[0].ToString();

app.MapSubscribeHandler();



app.MapPost("/A", [Topic("pubsub", "A")] (ILogger<Program> logger, MessageEvent item) => {
    string connetionString;
   SqlConnection cnn;
   connetionString = @"Server=MARIANA;Database=votes;Trusted_Connection=True;TrustServerCertificate=True;";
   cnn = new SqlConnection(connetionString);
   cnn.Open();
   Console.WriteLine("Connection Open  !");
    Console.WriteLine($"{item.MessageType}: {item.Message}");
    SqlCommand com;
    SqlDataReader datar;
    String sql = "";
    List<String> list = new List<String>();
    sql = "select dpi from voto";
    com = new SqlCommand(sql,cnn);
    datar = com.ExecuteReader();
    DateTime localDate = DateTime.Now;
    while  (datar.Read()){
        list.Add(Convert.ToString(datar.GetValue(0)));
    }
    for(int i=0;i<list.Count;i++)
            {
                Console.WriteLine(list[i]);
            }
    if (list.Contains(item.Message)){
        Console.WriteLine("Intento de fraude, usted ya votó");
    }
    else{
        using (SqlConnection connection = new SqlConnection("Server=MARIANA;Database=votes;Trusted_Connection=True;TrustServerCertificate=True;"))
{
    using (SqlCommand command = new SqlCommand())
    {
        command.Connection = connection;       
        command.CommandText = "INSERT into voto (dpi, voto, ip, fechahora) VALUES ( @dpi, @voto,@ip, @fechahora)";
        command.Parameters.AddWithValue("@dpi", item.Message);
        command.Parameters.AddWithValue("@voto", item.MessageType);
        command.Parameters.AddWithValue("@ip", myIP);
        command.Parameters.AddWithValue("@fechahora", localDate);

        try
        {
            connection.Open();
            int recordsAffected = command.ExecuteNonQuery();
        }
        catch(SqlException)
        {
            // error here
        }
        finally
        {
            connection.Close();
        }
    }
}
    }


    
    return Results.Ok();
});

app.MapPost("/B", [Topic("pubsub", "B")] (ILogger<Program> logger, MessageEvent item) => {
    string connetionString;
   SqlConnection cnn;
   connetionString = @"Server=MARIANA;Database=votes;Trusted_Connection=True;TrustServerCertificate=True;";
   cnn = new SqlConnection(connetionString);
   cnn.Open();
   Console.WriteLine("Connection Open  !");
    Console.WriteLine($"{item.MessageType}: {item.Message}");
    SqlCommand com;
    SqlDataReader datar;
    String sql = "";
    List<String> list = new List<String>();
    sql = "select dpi from voto";
    com = new SqlCommand(sql,cnn);
    datar = com.ExecuteReader();
    DateTime localDate = DateTime.Now;
    while  (datar.Read()){
        list.Add(Convert.ToString(datar.GetValue(0)));
    }
    for(int i=0;i<list.Count;i++)
            {
                Console.WriteLine(list[i]);
            }
    if (list.Contains(item.Message)){
        Console.WriteLine("Intento de fraude, usted ya votó");
    }
    else{
        using (SqlConnection connection = new SqlConnection("Server=MARIANA;Database=votes;Trusted_Connection=True;TrustServerCertificate=True;"))
{
    using (SqlCommand command = new SqlCommand())
    {
        command.Connection = connection;       
        command.CommandText = "INSERT into voto (dpi, voto, ip, fechahora) VALUES ( @dpi, @voto,@ip, @fechahora)";
        command.Parameters.AddWithValue("@dpi", item.Message);
        command.Parameters.AddWithValue("@voto", item.MessageType);
        command.Parameters.AddWithValue("@ip", myIP);
        command.Parameters.AddWithValue("@fechahora", localDate);

        try
        {
            connection.Open();
            int recordsAffected = command.ExecuteNonQuery();
        }
        catch(SqlException)
        {
            // error here
        }
        finally
        {
            connection.Close();
        }
    }
}
    }
return Results.Ok();

});
app.MapPost("/C", [Topic("pubsub", "C")] (ILogger<Program> logger, MessageEvent item) => {
    string connetionString;
   SqlConnection cnn;
   connetionString = @"Server=MARIANA;Database=votes;Trusted_Connection=True;TrustServerCertificate=True;";
   cnn = new SqlConnection(connetionString);
   cnn.Open();
   Console.WriteLine("Connection Open  !");
    Console.WriteLine($"{item.MessageType}: {item.Message}");
    SqlCommand com;
    SqlDataReader datar;
    String sql = "";
    List<String> list = new List<String>();
    sql = "select dpi from voto";
    com = new SqlCommand(sql,cnn);
    datar = com.ExecuteReader();
    DateTime localDate = DateTime.Now;
    while  (datar.Read()){
        list.Add(Convert.ToString(datar.GetValue(0)));
    }
    for(int i=0;i<list.Count;i++)
            {
                Console.WriteLine(list[i]);
            }
    if (list.Contains(item.Message)){
        Console.WriteLine("Intento de fraude, usted ya votó");
    }
    else{
        using (SqlConnection connection = new SqlConnection("Server=MARIANA;Database=votes;Trusted_Connection=True;TrustServerCertificate=True;"))
{
    using (SqlCommand command = new SqlCommand())
    {
        command.Connection = connection;       
        command.CommandText = "INSERT into voto (dpi, voto, ip, fechahora) VALUES ( @dpi, @voto,@ip, @fechahora)";
        command.Parameters.AddWithValue("@dpi", item.Message);
        command.Parameters.AddWithValue("@voto", "NULO");
        command.Parameters.AddWithValue("@ip", myIP);
        command.Parameters.AddWithValue("@fechahora", localDate);

        try
        {
            connection.Open();
            int recordsAffected = command.ExecuteNonQuery();
        }
        catch(SqlException)
        {
            // error here
        }
        finally
        {
            connection.Close();
        }
    }
}
    }
return Results.Ok();

});


app.Run();

internal record MessageEvent(string MessageType, string Message);