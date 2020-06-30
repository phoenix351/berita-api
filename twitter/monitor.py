from Database import Database_connection as db
database = db()
qy = """
INSERT INTO `berita_detail` (`judul`, `waktu`, `tag`, `isi`, `sumber`) VALUES
('Mentan harap tatanan normal baru pulihkan permintaan produk pertanian', '2020-06-07', '[normal baru,new normal,petani]', ' Dengan kebijakan normal baru utamanya di sektor pariwisata diharapkan dapat memulihkan permintaan produk pertanian Jakarta (ANTARA) - Menteri Pertanian Syahrul Yasin Limpo berharap tatanan normal baru dapat mendongkrak kesejahteraan petani dan memulihkan permintaan produk pertanian dengan dimulainya aktivitas hotel, restoran, katering (Horeka) dan perkantoran. Dampak yang ditimbulkan akibat pandemi ini masih dirasakan masyarakat, termasuk para petani. Faktor yang mempengaruhi petani yakni harga produk pertanian mengalami tekanan diakibatkan oleh panen raya musim tanam pertama. \"Kondisi ini menyebabkan deflasi kelompok bahan makanan dimana jumlah bahan pangan di lapangan banyak namun permintaan berkurang berakibat langsung dengan pendapatan petani,\" kata Syahrul dalam keterangan di Jakarta, Minggu. Selain itu, petani juga dihadapkan pada gangguan distribusi akibat Pembatasan Sosial Berskala Besar (PSBB), penurunan daya beli masyarakat, melemahnya sektor ekonomi yang terkait dengan sektor pertanian seperti Horeka dan perkantoran. Menurut Mentan, selama pandemi deflasi kelompok bahan makanan masih berimplikasi positif terhadap stabilitas sosial dan politik. Untuk mengurangi dampak ke pendapatan yang diterima petani, pemerintah memberikan bantuan sosial yang dapat mengkompensasi penurunan daya beli petani yang diakibatkan oleh penurunan harga produk pertanian. \"Dengan kebijakan normal baru utamanya di sektor pariwisata diharapkan dapat memulihkan permintaan produk pertanian sehingga dapat memperbaiki harga di tingkat petani,\" kata Syahrul. Kementerian Pertanian (Kementan) mencatat bahwa panen raya musim pertama sukses mengamankan stok pangan sehingga tidak terjadi gejolak kenaikan harga dan tersendatnya distribusi 11 bahan pokok khususnya dalam menghadapi Ramadhan dan Hari Raya Idul Fitri. Eksport komoditas pertanian juga masih tumbuh sebesar 12,6 persen. Namun demikian, Nilai Tukar Petani (NTP) diakui memang turun akibat pandemi. Syahrul menilai kondisi ini hanya sesaat. Menurut Mentan, kunci meningkatkan NTP adalah menyeimbangkan penawaran dan permintaan. Kebijakan pemerintah untuk membuka sektor pariwisata dan aktivitas perkantoran harus dipersiapkan dengan baik karena dengan keberhasilan kebijakan ini dapat berkontribusi terhadap perbaikan harga di tingkat petani. Menghadapi fenomena yang terjadi di kalangan petani, Mentan Syahrul mengatakan bahwa pihaknya sedang melakukan berbagai upaya salah satunya melakukan pengendalian dari sisi harga pertanian melalui koordinasi Bulog dan Kementerian Perdagangan. Pewarta: Mentari Dwi Gayati Editor: Ahmad Wijaya COPYRIGHT © ANTARA 2020 (adsbygoogle = window.adsbygoogle || []).push({}); ', 'antara')
"""
try:
    database.kursor.execute(qy)
    database.koneksi.commit()
    gen_id = database.kursor.lastrowid
    print(gen_id)
except Exception as ex:
    database.koneksi.rollback()
    print(ex)


