import qualified Data.ByteString.Char8 as BC
import Data.Hadoop.Types
import Data.List.Split (splitOn)
import qualified Data.Text as T
import Network.Hadoop.Hdfs
import Network.Hadoop.Read
import System.Environment

main :: IO ()
main = do
  args <- getArgs
  case args of
   ("print": path: user: nodes) -> printHdfsFile (mkConfig user nodes) path
   ("copyToLocal": destFile: path: user: nodes) -> copyHdfsFileToLocal destFile (mkConfig user nodes) path
   _ -> error "usage: command [cmd-args] path [user nodes]\n commands:\n print\n copyToLocal destFile"
  where
    mkConfig _ [] = Nothing
    mkConfig u ns = Just $ HadoopConfig (T.pack u) (map toEndpoint ns) Nothing
      where
        toEndpoint :: String -> Endpoint
        toEndpoint str = let es = splitOn ":" str in if length es == 2 then Endpoint (T.pack $ es !! 0) (read $ es !! 1) else error $ "illegal host: "++str

printHdfsFile :: Maybe HadoopConfig -> String -> IO ()
printHdfsFile = withHdfsReader print

copyHdfsFileToLocal :: FilePath -> Maybe HadoopConfig -> String -> IO ()
copyHdfsFileToLocal destFile = withHdfsReader $ BC.writeFile destFile

withHdfsReader :: (BC.ByteString -> IO ()) -> Maybe HadoopConfig -> String -> IO ()
withHdfsReader action config path =  do
  readHandle_ <- maybe runHdfs runHdfs' config $ openRead $ BC.pack path
  case readHandle_ of
   (Just readHandle) -> hdfsMapM_ action readHandle
   Nothing -> error "no read handle"
