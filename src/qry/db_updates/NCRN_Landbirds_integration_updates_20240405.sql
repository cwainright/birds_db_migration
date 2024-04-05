
-- 1. ncrn.Location
--     Problem: the x and y coordinates are mistakenly flipped for some locations.
--          All NCRN locations should have an x coord ~ -77 .
--     Solution: When `ncrn.Location.X_Coord_DD_NAD83` >0, swap `ncrn.Location.X_Coord_DD_NAD83` and `ncrn.Location.Y_Coord_DD_NAD83`.
BEGIN TRANSACTION [correct_lat_lon]
  BEGIN TRY
    UPDATE [NCRN_Landbirds].[ncrn].[Location] 
        SET X_Coord_DD_NAD83 = Y_Coord_DD_NAD83, 
            Y_Coord_DD_NAD83 = X_Coord_DD_NAD83
        WHERE X_Coord_DD_NAD83 >0;
      COMMIT TRANSACTION [correct_lat_lon]
  END TRY

  BEGIN CATCH
      ROLLBACK TRANSACTION [correct_lat_lon]
  END CATCH  

