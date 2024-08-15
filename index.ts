import { Database } from "bun:sqlite"

type LichessPuzzle = {
    puzzleId: string,
    fen: string,
    moves: string,
    rating: number,
    rating_deviation: number,
    popularity: number,
    nb_plays: number,
    themes: string,
    game_url: string,
    opening_tags: string,

}

type EncroissantPuzzle = {
    id: number, // sqlite integer
    fen: string, // sqlite text
    moves: string,
    rating: number,
    rating_deviation: number,
    popularity: number,
    nb_plays: number,
}
async function parseLichessCsv(db: Database): Promise<boolean> {
    const puzzle_csv = await Bun.file("./lichess_db_puzzle.csv").text()
    const rows = puzzle_csv.split("\n")
    for (const row of rows) {
        const [puzzle_id, fen, moves, rating, rating_deviation, popularity, nb_plays, themes, game_url, opening_tags] = row.split(",");
        const puzzle: LichessPuzzle = {
            puzzleId: puzzle_id,
            fen: fen,
            moves: moves,
            rating: Number(rating),
            rating_deviation: Number(rating_deviation),
            popularity: Number(popularity),
            nb_plays: Number(nb_plays),
            themes: themes,
            game_url: game_url,
            opening_tags: opening_tags
        }


        const query = db.query("INSERT INTO puzzles (puzzle_id, fen, moves, rating, rating_deviation, popularity, nb_plays, themes, game_url, opening_tags) VALUES ($puzzle_id,$fen,$moves,$rating, $rating_deviation,$popularity,$nb_plays,$themes,$game_url,$opening_tags)")
        query.run(
            {
                $puzzle_id: puzzle.puzzleId,
                $fen: puzzle.fen,
                $moves: puzzle.moves,
                $rating: puzzle.rating,
                $rating_deviation: puzzle.rating_deviation,
                $popularity: puzzle.popularity,
                $nb_plays: puzzle.nb_plays,
                $themes: puzzle.themes,
                $game_url: puzzle.game_url,
                $opening_tags: puzzle.opening_tags
            }
        )
    }
    return true
}

function initLichessDb(path: string): Database {
    const db = new Database(path, { create: true })
    db.exec("PRAGMA journal_mode = WAL")
    db.exec(`CREATE TABLE IF NOT EXISTS puzzles ( 
        id INTEGER NOT NULL PRIMARY KEY,
        puzzle_id TEXT,
        fen TEXT,
        moves TEXT, 
        rating INTEGER,
        rating_deviation INTEGER,
        popularity INTEGER,
        nb_plays INTEGER,
        themes TEXT,
        game_url TEXT,
        opening_tags TEXT
    )`)
    return db
}
function initEncroissantDb(path: string): Database {
    const db = new Database(path, { create: true })
    db.exec("PRAGMA journal_mode = WAL")
    db.exec(`CREATE TABLE IF NOT EXISTS puzzles ( 
        id INTEGER NOT NULL PRIMARY KEY,
        fen TEXT,
        moves TEXT, 
        rating INTEGER,
        rating_deviation INTEGER,
        popularity INTEGER,
        nb_plays INTEGER
    )`)
    return db
}

function filterIntoEncroissantByOpening(lichess_db: Database, encroissant_db: Database, opening: string) {
    const filter_query = lichess_db.query("SELECT * FROM puzzles WHERE opening_tags LIKE $opening")
    const opening_puzzles = filter_query.all({ $opening: `%${opening}%` }) as Array<LichessPuzzle>
    for (const puzzle of opening_puzzles) {
        const query = encroissant_db.query(`INSERT INTO puzzles 
                            (fen, moves, rating, rating_deviation, popularity, nb_plays) 
                            VALUES ($fen, $moves, $rating, $rating_deviation, $popularity, $nb_plays)`)
        query.run({
            $fen: puzzle.fen,
            $moves: puzzle.moves,
            $rating: puzzle.rating,
            $rating_deviation: puzzle.rating_deviation,
            $popularity: puzzle.popularity,
            $nb_plays: puzzle.nb_plays
        })
    }
}

async function main() {
    const lichess_db = initLichessDb("lichess_db.db3")
    // const done = await parseLichessCsv(lichess_db)
    const openings = ['petrov']
    for (const opening of openings) {
        const encroissant_db = initEncroissantDb(`${opening}_puzzles.db3`)
        filterIntoEncroissantByOpening(lichess_db, encroissant_db, opening)
        encroissant_db.close()
    }
    lichess_db.close()
}

const start = performance.now()
await main()
const end = performance.now()
console.log(`took: ${end - start}`)
