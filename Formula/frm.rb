class Frm < Formula
  desc "A columnar filetype conversion tool"
  homepage "https://github.com/maxvandijck/frm"
  url "https://github.com/maxvandijck/frm/releases/download/v0.0.2/frm-0.0.2-x86_64-apple-darwin.tar.gz"
  sha256 "c4298e2125ec4a24bcbeb9533289049b69bb8f6437dab0fe9bb519b6681fd250"
  version "0.0.2"

  def install
    bin.install "frm"
  end
end
