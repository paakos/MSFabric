CREATE PROCEDURE usp_trackNewWatermark
    @TableName VARCHAR(255),
    @Lastloadedtime   datetime2(3)
AS
BEGIN
    -- Update the control table with the provided table name and row count
    UPDATE [goldwh01].[audit].[Control]
    SET Lastloadedtime  = @Lastloadedtime
    WHERE TableName = @TableName;

    -- If the table name does not exist, insert a new row
    IF @@ROWCOUNT = 0
    BEGIN
        INSERT INTO [goldwh01].[audit].[Control] (TableName, Lastloadedtime)
        VALUES (@TableName, @Lastloadedtime);
    END
END;