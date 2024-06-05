# YOUTUBE-DATA-HARVESTING-and-WAREHOUSING-using-SQL-and-STREAMLIT
 Building a easiest way to access the details and retrieving data from YouTube API, storing and query the data using SQL as a Warehouse and displaying the data in the Streamlit app.


    _Set up a Streamlit app:_ 
                         Streamlit is a great choice for building data visualization and analysis tools quickly and easily. You can use Streamlit to create a simple UI where users can enter a YouTube channel ID, view the channel details, and select channels to migrate to the data warehouse.
    _
    Connect to the YouTube API_: 
                         You'll need to use the YouTube API to retrieve channel and video data. You can use the Google API client library for Python to make requests to the API.
    _
    Store and Clean data_ : 
                         Once you retrieve the data from the YouTube API, store it in a suitable format for temporary storage before migrating to the data warehouse. You can use pandas DataFrames or other in-memory data structures.
    
    _Migrate data to a SQL data warehouse:_
                         After you've collected data for multiple channels, you can migrate it to a SQL data warehouse. You can use a SQL database such as MySQL
    _
    Query the SQL data warehouse:_ 
                        You can use SQL queries to join the tables in the SQL data warehouse and retrieve data for specific channels based on user input. 
    _
    Display data in the Streamlit app: _
                         Finally, you can display the retrieved data in the Streamlit app. You can use Streamlit's data visualization features to create charts and graphs to help users analyze the data.
