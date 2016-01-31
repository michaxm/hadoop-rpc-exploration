import qualified Data.ByteString.Char8 as BC
import Data.Hadoop.Types
import Data.List.Split (splitOn)
import qualified Data.Text as T
import Network.Hadoop.Hdfs
import Network.Hadoop.Read
import System.Directory (doesFileExist)
import System.Environment
import System.IO (IOMode(..), withBinaryFile)

main :: IO ()
main = do
  args <- getArgs
  case args of
   ("print": path: user: nodes) -> printFile (mkConfig user nodes) path
   ("listLocations": path: user: nodes) -> listLocations (mkConfig user nodes) path
   ("copyToLocal": destFile: path: user: nodes) -> copyToLocal destFile (mkConfig user nodes) path
   _ -> error "usage: command [cmd-args] path [user nodes]\n commands:\n print\n copyToLocal destFile"
  where
    mkConfig _ [] = Nothing
    mkConfig u ns = Just $ HadoopConfig (T.pack u) (map toEndpoint ns) Nothing
      where
        toEndpoint :: String -> Endpoint
        toEndpoint str = let es = splitOn ":" str in if length es == 2 then Endpoint (T.pack $ es !! 0) (read $ es !! 1) else error $ "illegal host: "++str

printFile :: Maybe HadoopConfig -> String -> IO ()
printFile = withHdfsReader print

listLocations :: Maybe HadoopConfig -> String -> IO ()
listLocations config path = do
  locs <- maybe runHdfs runHdfs' config $ getBlockLocations $ BC.pack path
  print locs

copyToLocal :: FilePath -> Maybe HadoopConfig -> String -> IO ()
copyToLocal destFile config path = do
  destFileExists <- doesFileExist destFile
  if destFileExists
    then error $ destFile++" exists"
    else withBinaryFile destFile WriteMode $ \h -> withHdfsReader (BC.hPut h) config path

withHdfsReader :: (BC.ByteString -> IO ()) -> Maybe HadoopConfig -> String -> IO ()
withHdfsReader action config path =  do
  readHandle_ <- maybe runHdfs runHdfs' config $ openRead $ BC.pack path
  case readHandle_ of
   (Just readHandle) -> hdfsMapM_ action readHandle
   Nothing -> error "no read handle"
